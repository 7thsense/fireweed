#!/usr/bin/env bash
# Large-scale test tier: the fireweed and fireweed-turso lib tests under a `large` submodule and the
# Python formal tests in scripts/formal/large_test_*.py. Selection is by path
# only, so every test stays listed and runnable. The fast tier
# (fast-functional-gate.sh and the formal unit step) skips these tests.
#
# Fails when a tier runs zero tests, so a rename cannot empty the tier silently.
# Not listed in public-release-gates*.json: releases do not wait for this tier.
# Runs from .github/workflows/nightly.yml (daily schedule and manual dispatch),
# not on pull requests or pushes.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

toolchain="${FIREWEED_TOOLCHAIN:-1.99.0}"
log_dir="${FIREWEED_LARGE_TIER_LOG_DIR:-target/large-tier}"
mkdir -p "${log_dir}"

echo "--- large tier: fireweed lib tests under ::large:: ---"
cargo "+${toolchain}" test --locked -p fireweed --lib ::large:: 2>&1 | tee "${log_dir}/rust.log"
rust_ran="$(python3 - "${log_dir}/rust.log" <<'PY'
import re, sys
text = open(sys.argv[1]).read()
rows = re.findall(r"^test result: ok\. (\d+) passed; 0 failed; 0 ignored", text, re.MULTILINE)
print(sum(int(n) for n in rows))
PY
)"
if [[ "${rust_ran}" -eq 0 ]]; then
    echo "large tier ran 0 Rust tests; the ::large:: selection is empty" >&2
    exit 1
fi
echo "large tier Rust tests passed: ${rust_ran}"

echo "--- large tier: fireweed-turso lib tests under ::large:: (local feature, no services) ---"
cargo "+${toolchain}" test --locked -p fireweed-turso --features local --lib ::large:: 2>&1 | tee "${log_dir}/rust-turso.log"
turso_ran="$(python3 - "${log_dir}/rust-turso.log" <<'PY'
import re, sys
text = open(sys.argv[1]).read()
rows = re.findall(r"^test result: ok\. (\d+) passed; 0 failed; 0 ignored", text, re.MULTILINE)
print(sum(int(n) for n in rows))
PY
)"
if [[ "${turso_ran}" -eq 0 ]]; then
    echo "large tier ran 0 fireweed-turso tests; the ::large:: selection is empty" >&2
    exit 1
fi
echo "large tier fireweed-turso tests passed: ${turso_ran}"

echo "--- large tier: scripts/formal large_test_*.py ---"
python3 -m unittest discover -s scripts/formal -p 'large_test_*.py' -v 2>&1 | tee "${log_dir}/python.log"
python_ran="$(python3 - "${log_dir}/python.log" <<'PY'
import re, sys
text = open(sys.argv[1]).read()
match = re.search(r"^Ran (\d+) tests? in ", text, re.MULTILINE)
ok = re.search(r"^OK\b", text, re.MULTILINE)
print(int(match.group(1)) if match and ok else 0)
PY
)"
if [[ "${python_ran}" -eq 0 ]]; then
    echo "large tier ran 0 Python tests, or the run did not end OK" >&2
    exit 1
fi
echo "large tier Python tests passed: ${python_ran}"
echo "large tier passed"
