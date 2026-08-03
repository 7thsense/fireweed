#!/usr/bin/env python3
from __future__ import annotations

import re
import sys
from pathlib import Path


REQUIRED = {
    "FIREWEED_PERF_ENV",
    "FIREWEED_E3_RESIDENT",
    "FIREWEED_E3_LOAD_BATCH",
    "FIREWEED_E3_ACK_PUSHES",
    "FIREWEED_E3_ACK_CONCURRENCY",
    "FIREWEED_E3_LOAD_CONCURRENCY",
    "FIREWEED_RECOVERY_MAX_TAIL_COMMANDS",
    "FIREWEED_E3_STORAGE_TOPOLOGY",
    "FIREWEED_E3_STORAGE_TOPOLOGY_ID",
    "FIREWEED_E3_STORAGE_DURABILITY_CLAIM",
    "FIREWEED_E3_AUTHORITY_MODE",
    "FIREWEED_E3_SOURCE_REVISION",
    "FIREWEED_E3_RUN_ID",
    "FIREWEED_E3_COMPOSITION_FINGERPRINT",
    "FIREWEED_E3_FENCE_EVIDENCE_OUT",
    "FIREWEED_LEDGER_DIR",
    "FIREWEED_S3_TEST_ENDPOINT",
    "FIREWEED_S3_TEST_REGION",
    "FIREWEED_S3_TEST_BUCKET",
    "FIREWEED_S3_TEST_ACCESS_KEY",
    "FIREWEED_S3_TEST_SECRET_KEY",
}


def fail(message: str) -> None:
    raise SystemExit(f"E3 env forwarding invalid: {message}")


if len(sys.argv) != 2:
    fail("usage: verify-e3-env-forwarding.py <shell-source>")

text = Path(sys.argv[1]).read_text()
start_marker = "# E3_ENV_FORWARDING_START"
end_marker = "# E3_ENV_FORWARDING_END"
if text.count(start_marker) != 1 or text.count(end_marker) != 1:
    fail("requires exactly one forwarding marker pair")
block = text.split(start_marker, 1)[1].split(end_marker, 1)[0]
if not re.search(
    r"(?m)^env \\\n(?:  [A-Z0-9_]+=.* \\\n)+  cargo test -p fireweed-server --release --test performance_object_log_e3_live_tests \\\n    performance_object_log_e3_live_tests -- --nocapture\n$",
    block,
):
    fail("forwarding block is not one uninterrupted env command")
if re.search(r"(?m)^\s*#", block):
    fail("comments may not interrupt the continued env command")

keys = re.findall(r"(?m)^  ([A-Z0-9_]+)=.* \\$", block)
if len(keys) != len(set(keys)):
    fail("duplicate forwarded variable")
missing = sorted(REQUIRED - set(keys))
extra = sorted(set(keys) - REQUIRED)
if missing or extra:
    fail(f"missing={missing} extra={extra}")

print(f"E3 env forwarding valid: {len(keys)} required variables")
