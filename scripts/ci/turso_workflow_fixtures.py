#!/usr/bin/env python3
"""Bound the focused Turso lane's exception to its required correctness fixtures."""
import argparse
from pathlib import Path
import re
import textwrap

ROOT = Path(__file__).resolve().parents[2]
POSTGRES_SERVICE = textwrap.dedent('''
    services:
      postgres:
        image: postgres:16-alpine
        env:
          POSTGRES_USER: fireweed
          POSTGRES_PASSWORD: fireweed-test-only
          POSTGRES_DB: fireweed
        ports:
          - 5432:5432
        options: >-
          --health-cmd "pg_isready -U fireweed -d fireweed"
          --health-interval 5s
          --health-timeout 5s
          --health-retries 12
''').strip()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def step(workflow: str, name: str) -> str:
    marker = "      - name: " + name + "\n"
    require(workflow.count(marker) == 1, "missing/duplicate fixture step: " + name)
    return workflow.split(marker, 1)[1].split("\n      - ", 1)[0]


def validate(workflow: str) -> None:
    # This is deliberately an exact, small exception, not a general YAML service
    # parser or permission for other hosted lanes to provision arbitrary services.
    markers = re.findall(r"^([ ]*)services:\s*$", workflow, re.MULTILINE)
    require(markers == ["    "], "only one focused-job services block is permitted")
    require("    steps:\n" in workflow, "focused steps missing")
    block = "    services:\n" + workflow.split("    services:\n", 1)[1].split("    steps:\n", 1)[0]
    require(textwrap.dedent(block).strip() == POSTGRES_SERVICE, "only the reviewed PostgreSQL readiness/service configuration is permitted")
    provision = step(workflow, "Provision matrix PostgreSQL/S3 fixtures")
    require("timeout-minutes: 5" in provision, "fixture startup must be bounded")
    # S3 is the checksum-pinned RustFS binary started by the qualification script, not a container.
    require("docker" not in provision, "S3 fixture must not use containers")
    for marker in (
        "bash scripts/ci/s3-qualification-endpoint.sh provision",
        "bash scripts/ci/s3-qualification-endpoint.sh verify-isolation",
        'source "${FIREWEED_S3_SECRET_DIR}/credentials.env"',
        "FIREWEED_PG_TEST_URL=%s",
        "FIREWEED_S3_TEST_ENDPOINT FIREWEED_S3_TEST_BUCKET FIREWEED_S3_TEST_REGION FIREWEED_S3_TEST_ACCESS_KEY FIREWEED_S3_TEST_SECRET_KEY",
    ):
        require(marker in provision, "required fixture setup missing: " + marker)
    secret_setting = "          FIREWEED_S3_SECRET_DIR: ${{ runner.temp }}/fireweed-turso-s3-${{ github.run_id }}-${{ github.run_attempt }}"
    require(secret_setting in provision, "provision must use step-level runner-owned temporary storage")
    mask_arguments = '"$pg_password" "$pg_url" "$FIREWEED_S3_TEST_ACCESS_KEY" "$FIREWEED_S3_TEST_SECRET_KEY"'
    require("::add-mask::" in provision and mask_arguments in provision and '>> "$GITHUB_ENV"' in provision, "credential masking/export missing")
    require(provision.index("::add-mask::") < provision.index('>> "$GITHUB_ENV"'), "mask credentials before exporting test environment")
    require(workflow.index("Provision matrix PostgreSQL/S3 fixtures") < workflow.index("- name: Fireweed 12-cell Turso route tests"), "fixtures must precede the full matrix")
    cleanup = step(workflow, "Tear down matrix S3 fixture")
    require(secret_setting in cleanup, "cleanup must use the same step-level temporary storage")
    require(workflow.count(secret_setting.strip()) == 2, "runner context must appear only in the two fixture steps")
    require("if: always()" in cleanup, "S3 cleanup must run after failures")
    require("timeout-minutes: 2" in cleanup, "S3 cleanup must be bounded")
    require("run: bash scripts/ci/s3-qualification-endpoint.sh teardown" in cleanup, "owned S3 teardown missing")


def self_test(workflow: str) -> None:
    validate(workflow)
    mutations = {
        "extra service": ("    steps:\n", "      redis:\n        image: redis:latest\n    steps:\n"),
        "different PostgreSQL image": ("image: postgres:16-alpine", "image: postgres:latest"),
        "missing PostgreSQL health": ('--health-cmd "pg_isready -U fireweed -d fireweed"', "--health-cmd true"),
        "missing S3 provision/CAS": ("s3-qualification-endpoint.sh provision", "s3-qualification-endpoint.sh status"),
        "container S3 fixture": (
            "bash scripts/ci/s3-qualification-endpoint.sh provision",
            "docker pull example/s3:latest\n          bash scripts/ci/s3-qualification-endpoint.sh provision",
        ),
        "unowned secret directory": ("${{ runner.temp }}/fireweed-turso-s3-", "/tmp/unowned-fireweed-turso-s3-"),
        "missing PG environment": ("FIREWEED_PG_TEST_URL=%s", "IGNORED_PG_URL=%s"),
        "missing S3 environment": ("FIREWEED_S3_TEST_BUCKET FIREWEED_S3_TEST_REGION", "FIREWEED_S3_TEST_REGION"),
        "unmasked credentials": ("::add-mask::", "::notice::"),
        "unmasked S3 secret": ('"$FIREWEED_S3_TEST_SECRET_KEY"', '"redacted"'),
        "conditional cleanup": ("if: always()", "if: success()"),
        "missing cleanup": ("s3-qualification-endpoint.sh teardown", "s3-qualification-endpoint.sh status"),
    }
    for label, (original, replacement) in mutations.items():
        require(original in workflow, "fixture mutation did not apply: " + label)
        try:
            validate(workflow.replace(original, replacement, 1))
        except ValueError:
            continue
        raise ValueError("unsafe fixture workflow accepted: " + label)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    workflow = (ROOT / ".github/workflows/turso.yml").read_text()
    validate(workflow)
    if args.self_test:
        self_test(workflow)
    print("Turso correctness fixtures: PostgreSQL readiness, pinned native-CAS RustFS, masked environment and unconditional cleanup verified")


if __name__ == "__main__":
    main()
