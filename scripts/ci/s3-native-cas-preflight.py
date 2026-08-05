#!/usr/bin/env python3
"""Two-writer native CAS preflight for S3-compatible qualification endpoints.

Proves the endpoint enforces:
  - native atomic conditional create (PutObject + If-None-Match: *)
  - native atomic conditional update (PutObject + If-Match: <etag>)

Exactly one of two concurrent create-only writers may succeed for the same key.
A second sequential create-only put must return HTTP 412.

Credentials are read from environment variables only; this tool never prints
secret values. Exit 0 only when every required observation succeeds.

Usage:
  FIREWEED_S3_TEST_ENDPOINT=... \\
  FIREWEED_S3_TEST_BUCKET=... \\
  FIREWEED_S3_TEST_REGION=us-east-1 \\
  FIREWEED_S3_TEST_ACCESS_KEY=... \\
  FIREWEED_S3_TEST_SECRET_KEY=... \\
  python3 scripts/ci/s3-native-cas-preflight.py [--json-out PATH]
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any


ALGORITHM = "AWS4-HMAC-SHA256"
SERVICE = "s3"
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


class PreflightError(RuntimeError):
    pass


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise PreflightError(f"required environment variable {name} is unset or empty")
    return value


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def signing_key(secret: str, datestamp: str, region: str) -> bytes:
    k_date = hmac_sha256(("AWS4" + secret).encode("utf-8"), datestamp)
    k_region = hmac.new(k_date, region.encode("utf-8"), hashlib.sha256).digest()
    k_service = hmac.new(k_region, SERVICE.encode("utf-8"), hashlib.sha256).digest()
    return hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()


class S3Client:
    def __init__(
        self,
        endpoint: str,
        region: str,
        access_key: str,
        secret_key: str,
    ) -> None:
        parsed = urllib.parse.urlparse(endpoint)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise PreflightError(f"invalid endpoint URL: {endpoint!r}")
        self.endpoint = endpoint.rstrip("/")
        self.scheme = parsed.scheme
        self.host = parsed.netloc
        self.region = region
        self.access_key = access_key
        self.secret_key = secret_key

    def request(
        self,
        method: str,
        path: str,
        *,
        body: bytes = b"",
        extra_headers: dict[str, str] | None = None,
        query: str = "",
    ) -> tuple[int, dict[str, str], bytes]:
        if not path.startswith("/"):
            path = "/" + path
        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        datestamp = now.strftime("%Y%m%d")
        payload_hash = sha256_hex(body)

        headers: dict[str, str] = {
            "host": self.host,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
            "content-length": str(len(body)),
        }
        if extra_headers:
            for key, value in extra_headers.items():
                headers[key.lower()] = value

        signed_header_names = sorted(headers)
        canonical_headers = "".join(f"{k}:{headers[k].strip()}\n" for k in signed_header_names)
        signed_headers = ";".join(signed_header_names)
        canonical_request = "\n".join(
            [
                method,
                path,
                query,
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )
        credential_scope = f"{datestamp}/{self.region}/{SERVICE}/aws4_request"
        string_to_sign = "\n".join(
            [
                ALGORITHM,
                amz_date,
                credential_scope,
                sha256_hex(canonical_request.encode("utf-8")),
            ]
        )
        signature = hmac.new(
            signing_key(self.secret_key, datestamp, self.region),
            string_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        authorization = (
            f"{ALGORITHM} Credential={self.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )

        url = f"{self.endpoint}{path}"
        if query:
            url = f"{url}?{query}"
        request_headers = {k: headers[k] for k in headers if k != "host"}
        request_headers["Authorization"] = authorization
        # Preserve original header names for conditional headers.
        if extra_headers:
            for key, value in extra_headers.items():
                request_headers[key] = value

        req = urllib.request.Request(url, data=body if body else None, method=method)
        for key, value in request_headers.items():
            req.add_header(key, value)

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                resp_body = resp.read()
                resp_headers = {k.lower(): v for k, v in resp.headers.items()}
                return int(resp.status), resp_headers, resp_body
        except urllib.error.HTTPError as err:
            resp_body = err.read()
            resp_headers = {k.lower(): v for k, v in err.headers.items()}
            return int(err.code), resp_headers, resp_body
        except urllib.error.URLError as err:
            raise PreflightError(f"request to {url} failed: {err}") from err


def ensure_bucket(client: S3Client, bucket: str) -> dict[str, Any]:
    status, headers, body = client.request("PUT", f"/{bucket}")
    # 200 created, 409 already owned/exists are both acceptable for qualification.
    if status not in (200, 409):
        snippet = body[:200].decode("utf-8", errors="replace")
        raise PreflightError(f"create bucket failed status={status}: {snippet}")
    return {
        "status": status,
        "etag": headers.get("etag"),
    }


def put_create_only(
    client: S3Client, bucket: str, key: str, body: bytes
) -> tuple[int, dict[str, str], bytes]:
    return client.request(
        "PUT",
        f"/{bucket}/{key}",
        body=body,
        extra_headers={"If-None-Match": "*"},
    )


def put_if_match(
    client: S3Client, bucket: str, key: str, body: bytes, etag: str
) -> tuple[int, dict[str, str], bytes]:
    return client.request(
        "PUT",
        f"/{bucket}/{key}",
        body=body,
        extra_headers={"If-Match": etag},
    )


def get_object(client: S3Client, bucket: str, key: str) -> tuple[int, dict[str, str], bytes]:
    return client.request("GET", f"/{bucket}/{key}")


def delete_object(client: S3Client, bucket: str, key: str) -> int:
    status, _, _ = client.request("DELETE", f"/{bucket}/{key}")
    return status


def concurrent_create_only(
    client: S3Client, bucket: str, key: str
) -> dict[str, Any]:
    barrier = threading.Barrier(2)
    results: list[tuple[str, int, dict[str, str]]] = []
    lock = threading.Lock()

    def worker(label: str, payload: bytes) -> None:
        barrier.wait(timeout=10)
        status, headers, _ = put_create_only(client, bucket, key, payload)
        with lock:
            results.append((label, status, headers))

    t1 = threading.Thread(
        target=worker, args=("writer-a", b"writer-a-body"), name="cas-writer-a"
    )
    t2 = threading.Thread(
        target=worker, args=("writer-b", b"writer-b-body"), name="cas-writer-b"
    )
    t1.start()
    t2.start()
    t1.join(timeout=30)
    t2.join(timeout=30)
    if t1.is_alive() or t2.is_alive():
        raise PreflightError("concurrent create-only writers did not finish within 30s")
    if len(results) != 2:
        raise PreflightError(f"expected 2 concurrent results, got {results!r}")

    successes = [(label, status, headers) for label, status, headers in results if status in (200, 204)]
    failures = [(label, status) for label, status, _ in results if status not in (200, 204)]
    if len(successes) != 1:
        raise PreflightError(
            "two-writer create-only race must admit exactly one winner; "
            f"results={[(l, s) for l, s, _ in results]}"
        )
    # Loser must be precondition failure (412). Some stacks also use 409.
    loser_statuses = {status for _, status in failures}
    if not failures or not loser_statuses.issubset({412, 409}):
        raise PreflightError(
            "two-writer create-only loser must return 412 (or 409); "
            f"results={[(l, s) for l, s, _ in results]}"
        )
    winner_label, winner_status, winner_headers = successes[0]
    return {
        "writer_results": [{"writer": l, "status": s} for l, s, _ in results],
        "winner": winner_label,
        "winner_status": winner_status,
        "winner_etag": winner_headers.get("etag"),
        "loser_statuses": sorted(loser_statuses),
    }


def run_preflight() -> dict[str, Any]:
    endpoint = require_env("FIREWEED_S3_TEST_ENDPOINT")
    bucket = require_env("FIREWEED_S3_TEST_BUCKET")
    region = os.environ.get("FIREWEED_S3_TEST_REGION", "us-east-1").strip() or "us-east-1"
    access_key = require_env("FIREWEED_S3_TEST_ACCESS_KEY")
    secret_key = require_env("FIREWEED_S3_TEST_SECRET_KEY")

    client = S3Client(endpoint, region, access_key, secret_key)
    run_nonce = f"{int(time.time())}-{os.getpid()}"
    prefix = f"fireweed-p1s-cas/{run_nonce}"
    create_key = f"{prefix}/create-only"
    race_key = f"{prefix}/two-writer-race"
    update_key = f"{prefix}/conditional-update"

    bucket_obs = ensure_bucket(client, bucket)

    # Sequential create-only: first wins, second must 412.
    first_status, first_headers, _ = put_create_only(
        client, bucket, create_key, b"create-only-first"
    )
    if first_status not in (200, 204):
        raise PreflightError(
            f"first create-only put expected 200, got {first_status}"
        )
    second_status, _, _ = put_create_only(
        client, bucket, create_key, b"create-only-second-must-fail"
    )
    if second_status != 412:
        raise PreflightError(
            f"second create-only put must return 412 Precondition Failed, got {second_status} "
            "(endpoint does not enforce If-None-Match: *)"
        )
    get_status, _, body = get_object(client, bucket, create_key)
    if get_status != 200 or body != b"create-only-first":
        raise PreflightError(
            "create-only winner body was overwritten or unreadable after second put"
        )

    race = concurrent_create_only(client, bucket, race_key)

    # Conditional update: If-Match with current etag succeeds; stale etag fails.
    put_status, put_headers, _ = put_create_only(
        client, bucket, update_key, b"update-v1"
    )
    if put_status not in (200, 204):
        raise PreflightError(f"seed conditional-update object failed status={put_status}")
    etag = put_headers.get("etag")
    if not etag:
        # Some endpoints omit ETag on PUT; re-GET.
        g_status, g_headers, _ = get_object(client, bucket, update_key)
        if g_status != 200:
            raise PreflightError("could not read etag for conditional update seed")
        etag = g_headers.get("etag")
    if not etag:
        raise PreflightError("endpoint did not return ETag required for If-Match preflight")

    match_status, match_headers, _ = put_if_match(
        client, bucket, update_key, b"update-v2", etag
    )
    if match_status not in (200, 204):
        raise PreflightError(
            f"If-Match with current etag must succeed, got {match_status}"
        )
    new_etag = match_headers.get("etag") or etag
    stale_status, _, _ = put_if_match(
        client, bucket, update_key, b"update-stale", etag
    )
    if stale_status != 412:
        # If the first If-Match rotated etag and second used old etag, 412 is required.
        # If endpoint returned same etag, try a known-bad etag.
        if stale_status in (200, 204):
            raise PreflightError(
                "If-Match with stale etag must return 412; endpoint overwrote instead"
            )
        bad_status, _, _ = put_if_match(
            client, bucket, update_key, b"update-bad", '"fireweed-stale-etag"'
        )
        if bad_status != 412:
            raise PreflightError(
                f"If-Match with invalid etag must return 412, got {bad_status}"
            )
        stale_status = bad_status

    # Best-effort cleanup of probe keys (failure does not fail preflight).
    for key in (create_key, race_key, update_key):
        try:
            delete_object(client, bucket, key)
        except PreflightError:
            pass

    return {
        "status": "passed",
        "endpoint": endpoint,
        "bucket": bucket,
        "region": region,
        "native_atomic_conditional_create": True,
        "native_atomic_conditional_update": True,
        "sequential_create_only": {
            "first_status": first_status,
            "second_status": second_status,
            "winner_body_preserved": True,
            "key": create_key,
        },
        "two_writer_create_only_race": race,
        "conditional_update": {
            "if_match_current_status": match_status,
            "if_match_stale_status": stale_status,
            "key": update_key,
            "seed_etag_present": True,
            "post_match_etag_present": bool(new_etag),
        },
        "bucket_ensure": bucket_obs,
        "probe_prefix": prefix,
        "tls_mode": "https" if endpoint.startswith("https://") else "plaintext",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--json-out",
        help="Write full preflight observation JSON to this path (non-secret)",
    )
    args = parser.parse_args()
    try:
        result = run_preflight()
    except PreflightError as err:
        print(f"s3-native-cas-preflight: FAIL: {err}", file=sys.stderr)
        return 1
    except Exception as err:  # noqa: BLE001 - surface unexpected failures closed
        print(f"s3-native-cas-preflight: ERROR: {err}", file=sys.stderr)
        return 1

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump(result, fh, indent=2, sort_keys=True)
            fh.write("\n")

    # Human summary never includes credential values.
    print("s3-native-cas-preflight: PASS")
    print(f"  endpoint={result['endpoint']}")
    print(f"  bucket={result['bucket']}")
    print(f"  region={result['region']}")
    print(
        "  sequential create-only: "
        f"{result['sequential_create_only']['first_status']} then "
        f"{result['sequential_create_only']['second_status']}"
    )
    race = result["two_writer_create_only_race"]
    print(
        f"  two-writer race: winner={race['winner']} "
        f"losers={race['loser_statuses']}"
    )
    print(
        "  conditional update: "
        f"match={result['conditional_update']['if_match_current_status']} "
        f"stale={result['conditional_update']['if_match_stale_status']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
