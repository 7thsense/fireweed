"""Minimal Fireweed RESP helpers over redis-py.

Field names and units match TD-006 / fireweed-resp:
- priority: decimal i64 string (bootstrap queues: ascending)
- not_before: UTC epoch milliseconds string
- client_item_key: stable producer identity for pending replace
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

import redis
from redis.exceptions import ResponseError


DEFAULT_GROUP = "workers"
DEFAULT_CONSUMER = "worker-1"


@dataclass(frozen=True)
class WorkItem:
    """One XADD body for a work item."""

    client_item_key: str
    priority: int
    not_before: int | None = None
    payload: str = ""
    group_key: str | None = None
    extra: Mapping[str, str] | None = None

    def fields(self) -> dict[str, str]:
        body: dict[str, str] = {
            "client_item_key": self.client_item_key,
            "priority": str(self.priority),
            "payload": self.payload,
        }
        if self.not_before is not None:
            body["not_before"] = str(self.not_before)
        if self.group_key is not None:
            body["group_key"] = self.group_key
        if self.extra:
            for k, v in self.extra.items():
                body[str(k)] = str(v)
        return body


def connect(url: str = "redis://127.0.0.1:8080") -> redis.Redis:
    """Open a redis-py client (decode_responses for string fields)."""
    return redis.Redis.from_url(url, decode_responses=True)


def now_ms() -> int:
    return int(time.time() * 1000)


def xadd(r: redis.Redis, queue: str, fields: Mapping[str, str]) -> str:
    """XADD <queue> * field value ... → server-assigned item id."""
    return r.xadd(queue, dict(fields))


def pipeline_xadd(
    r: redis.Redis,
    queue: str,
    items: Sequence[WorkItem | Mapping[str, str]],
    *,
    batch_size: int = 1000,
) -> list[str]:
    """Pipelined XADD. Fireweed is not Redis MULTI; use transaction=False."""
    ids: list[str] = []
    batch_size = max(1, batch_size)
    for start in range(0, len(items), batch_size):
        chunk = items[start : start + batch_size]
        pipe = r.pipeline(transaction=False)
        for item in chunk:
            fields = item.fields() if isinstance(item, WorkItem) else dict(item)
            pipe.xadd(queue, fields)
        ids.extend(pipe.execute())
    return ids


def normalize_xreadgroup(reply: Any) -> list[tuple[str, dict[str, str]]]:
    """Normalize redis-py xreadgroup reply to [(item_id, fields), ...]."""
    if not reply:
        return []
    # [[stream, [(id, {field: value}), ...]]]
    out: list[tuple[str, dict[str, str]]] = []
    for _stream, entries in reply:
        for item_id, fields in entries:
            out.append((item_id, dict(fields)))
    return out


def claim_batch(
    r: redis.Redis,
    queue: str,
    *,
    group: str = DEFAULT_GROUP,
    consumer: str = DEFAULT_CONSUMER,
    count: int = 100,
) -> list[tuple[str, dict[str, str]]]:
    """XREADGROUP GROUP g c COUNT n STREAMS queue > — priority-ordered eligible work."""
    reply = r.xreadgroup(group, consumer, streams={queue: ">"}, count=count)
    return normalize_xreadgroup(reply)


def complete(
    r: redis.Redis,
    queue: str,
    *item_ids: str,
    group: str = DEFAULT_GROUP,
) -> int:
    """XACK — finalize Complete only (all-or-nothing)."""
    if not item_ids:
        return 0
    return int(r.xack(queue, group, *item_ids))


def xinfo_stream_raw(r: redis.Redis, queue: str) -> dict[str, Any]:
    """XINFO STREAM without redis-py's Redis-shaped response callback.

    Fireweed returns a subset of fields (e.g. length, resident-terminal-count)
    and omits Redis-only keys such as last-entry that redis-py's parser requires.
    """
    raw = r.execute_command("XINFO", "STREAM", queue)
    if not raw:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    # flat [k, v, k, v, ...]
    out: dict[str, Any] = {}
    it = iter(raw)
    for k, v in zip(it, it):
        out[str(k)] = v
    return out


def queue_status(
    r: redis.Redis,
    queue: str,
    *,
    group: str = DEFAULT_GROUP,
) -> dict[str, Any]:
    """Live depth proxies: XLEN, XINFO STREAM, XPENDING summary."""
    xlen = int(r.xlen(queue))
    try:
        xinfo = xinfo_stream_raw(r, queue)
    except ResponseError:
        xinfo = {}
    try:
        xpending = r.xpending(queue, group)
    except ResponseError:
        xpending = None
    return {"xlen": xlen, "xinfo": xinfo, "xpending": xpending}


def fw_hgetall(r: redis.Redis, queue: str, client_item_key: str) -> dict[str, str]:
    """FW.HGETALL — live item fields by client_item_key (Pending or Leased)."""
    raw = r.execute_command("FW.HGETALL", queue, client_item_key)
    if not raw:
        return {}
    # flat [k, v, k, v, ...]
    it = iter(raw)
    return {k: v for k, v in zip(it, it)}


def xclaim_renew(
    r: redis.Redis,
    queue: str,
    item_id: str,
    lease_token: str,
    *,
    group: str = DEFAULT_GROUP,
) -> Any:
    """XCLAIM with consumer=lease_token renews without charging a delivery."""
    return r.execute_command(
        "XCLAIM", queue, group, lease_token, 0, item_id, "JUSTID"
    )


def xautoclaim(
    r: redis.Redis,
    queue: str,
    *,
    group: str = DEFAULT_GROUP,
    consumer: str = DEFAULT_CONSUMER,
    count: int = 100,
    start: str = "0-0",
) -> Any:
    """XAUTOCLAIM — reclaim after lease expiry (min-idle ignored server-side)."""
    return r.execute_command(
        "XAUTOCLAIM", queue, group, consumer, 0, start, "COUNT", count
    )


def fireweed_error_kind(exc: BaseException) -> str | None:
    text = str(exc).lower()
    for kind in (
        "stale_lease",
        "superseded",
        "invalid",
        "terminal",
        "unavailable",
    ):
        if kind in text:
            return kind
    return None


def timed(fn, *args, **kwargs) -> tuple[Any, float]:
    """Return (result, wall_seconds)."""
    t0 = time.perf_counter()
    result = fn(*args, **kwargs)
    return result, time.perf_counter() - t0


def percentile(samples: Sequence[float], p: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    if len(ordered) == 1:
        return ordered[0]
    idx = min(len(ordered) - 1, max(0, int(round((p / 100.0) * (len(ordered) - 1)))))
    return ordered[idx]
