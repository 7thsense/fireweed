#!/usr/bin/env bash
# Isolated temporary-Git-repository tests for promote-governed-evidence.sh (P17e).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CASE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/fireweed-promote-evidence.XXXXXX")"
cleanup() { rm -rf "$CASE_ROOT"; }
trap cleanup EXIT

fail() { echo "promote-governed-evidence-test: $*" >&2; exit 1; }

repo="$CASE_ROOT/source"
mkdir -p "$repo/docs/perf/evidence/current"
git -C "$CASE_ROOT" init -q source
git -C "$repo" config user.name Test
git -C "$repo" config user.email test@example.invalid
git -C "$repo" remote add origin "https://example.invalid/fireweed.git"
printf '%s\n' 'target/' '.ddx/agent-logs/' 'node_modules/' '__pycache__/' 'examples/python-resp/.venv/' >"$repo/.gitignore"
printf 'seed\n' >"$repo/README.md"
printf 'historical\n' >"$repo/docs/perf/evidence/historical.txt"
git -C "$repo" add .
git -C "$repo" commit -qm seed
S="$(git -C "$repo" rev-parse HEAD)"

bundle="$CASE_ROOT/bundle"
mkdir -p "$bundle/docs/perf/evidence/current"
printf 'new-current-evidence\n' >"$bundle/docs/perf/evidence/current/note.json"
digest="$(sha256sum "$bundle/docs/perf/evidence/current/note.json" | awk '{print $1}')"
allowlist="$CASE_ROOT/allowlist.json"
python3 - "$allowlist" "$S" "$digest" <<'PY'
import json, sys
path, source, digest = sys.argv[1:]
json.dump(
    {
        "expected_source": source,
        "campaign": "storage",
        "paths": ["docs/perf/evidence/current/note.json"],
        "historical_paths": ["docs/perf/evidence/historical.txt"],
        "digests": {"docs/perf/evidence/current/note.json": digest},
    },
    open(path, "w", encoding="utf-8"),
    indent=2,
)
PY

# Positive promotion
promo="$CASE_ROOT/promo-ok"
E="$(bash "$SCRIPT_DIR/promote-governed-evidence.sh" \
  --source-root "$repo" \
  --expected-source "$S" \
  --expected-remote origin \
  --expected-ref HEAD \
  --campaign storage \
  --bundle-root "$bundle" \
  --allowlist "$allowlist" \
  --promotion-root "$promo" | tail -n 1)"
[[ "$E" =~ ^[0-9a-f]{40}$ ]] || fail "promoter did not report E: ${E}"
[[ "$E" != "$S" ]] || fail "E must differ from S"
git -C "$promo" cat-file -e "$E^{commit}"
git -C "$promo" merge-base --is-ancestor "$S" "$E" || fail "S not ancestor of E"
diff_names="$(git -C "$promo" diff --name-only "$S" "$E")"
[[ "$diff_names" == "docs/perf/evidence/current/note.json" ]] || fail "diff(S,E) not exact: $diff_names"
grep -Fq "Measured-source: $S" <<<"$(git -C "$promo" log -1 --format=%B "$E")" || fail "missing S metadata"

expect_fail() {
  local label="$1"; shift
  if "$@" >"$CASE_ROOT/$label.out" 2>&1; then
    fail "$label unexpectedly passed"
  fi
}

# Tampered digest
tampered="$CASE_ROOT/allowlist-tamper.json"
python3 - "$tampered" "$S" <<'PY'
import json, sys
path, source = sys.argv[1:]
json.dump(
    {
        "expected_source": source,
        "campaign": "storage",
        "paths": ["docs/perf/evidence/current/note.json"],
        "historical_paths": ["docs/perf/evidence/historical.txt"],
        "digests": {"docs/perf/evidence/current/note.json": "0"*64},
    },
    open(path, "w", encoding="utf-8"),
)
PY
expect_fail tampered bash "$SCRIPT_DIR/promote-governed-evidence.sh" \
  --source-root "$repo" --expected-source "$S" --expected-remote origin --expected-ref HEAD \
  --campaign storage --bundle-root "$bundle" --allowlist "$tampered" \
  --promotion-root "$CASE_ROOT/promo-tamper"

# Extra bundle path
cp -a "$bundle" "$CASE_ROOT/bundle-extra"
printf 'extra\n' >"$CASE_ROOT/bundle-extra/extra.txt"
expect_fail extra bash "$SCRIPT_DIR/promote-governed-evidence.sh" \
  --source-root "$repo" --expected-source "$S" --expected-remote origin --expected-ref HEAD \
  --campaign storage --bundle-root "$CASE_ROOT/bundle-extra" --allowlist "$allowlist" \
  --promotion-root "$CASE_ROOT/promo-extra"

# Missing path
mkdir -p "$CASE_ROOT/bundle-missing"
expect_fail missing bash "$SCRIPT_DIR/promote-governed-evidence.sh" \
  --source-root "$repo" --expected-source "$S" --expected-remote origin --expected-ref HEAD \
  --campaign storage --bundle-root "$CASE_ROOT/bundle-missing" --allowlist "$allowlist" \
  --promotion-root "$CASE_ROOT/promo-missing"

# Historical overwrite attempt
hist_allow="$CASE_ROOT/allowlist-hist.json"
printf 'overwrite\n' >"$bundle/docs/perf/evidence/historical.txt"
hist_digest="$(sha256sum "$bundle/docs/perf/evidence/historical.txt" | awk '{print $1}')"
python3 - "$hist_allow" "$S" "$hist_digest" <<'PY'
import json, sys
path, source, digest = sys.argv[1:]
json.dump(
    {
        "expected_source": source,
        "campaign": "storage",
        "paths": ["docs/perf/evidence/historical.txt"],
        "historical_paths": ["docs/perf/evidence/historical.txt"],
        "digests": {"docs/perf/evidence/historical.txt": digest},
    },
    open(path, "w", encoding="utf-8"),
)
PY
expect_fail historical bash "$SCRIPT_DIR/promote-governed-evidence.sh" \
  --source-root "$repo" --expected-source "$S" --expected-remote origin --expected-ref HEAD \
  --campaign storage --bundle-root "$bundle" --allowlist "$hist_allow" \
  --promotion-root "$CASE_ROOT/promo-hist"

# Wrong S
expect_fail wrong_s bash "$SCRIPT_DIR/promote-governed-evidence.sh" \
  --source-root "$repo" --expected-source "0000000000000000000000000000000000000000" \
  --expected-remote origin --expected-ref HEAD \
  --campaign storage --bundle-root "$bundle" --allowlist "$allowlist" \
  --promotion-root "$CASE_ROOT/promo-wrong-s"

# Dirty source
printf 'dirty\n' >>"$repo/README.md"
expect_fail dirty bash "$SCRIPT_DIR/promote-governed-evidence.sh" \
  --source-root "$repo" --expected-source "$S" --expected-remote origin --expected-ref HEAD \
  --campaign storage --bundle-root "$bundle" --allowlist "$allowlist" \
  --promotion-root "$CASE_ROOT/promo-dirty"
git -C "$repo" checkout -- README.md

# In-repo promotion root
expect_fail in_repo bash "$SCRIPT_DIR/promote-governed-evidence.sh" \
  --source-root "$repo" --expected-source "$S" --expected-remote origin --expected-ref HEAD \
  --campaign storage --bundle-root "$bundle" --allowlist "$allowlist" \
  --promotion-root "$repo/promo-inside"

# Symlink escape in bundle
mkdir -p "$CASE_ROOT/bundle-link"
ln -s /etc/passwd "$CASE_ROOT/bundle-link/docs"
# create structure carefully
rm -rf "$CASE_ROOT/bundle-link"
mkdir -p "$CASE_ROOT/bundle-link/docs/perf/evidence/current"
ln -sf /etc/passwd "$CASE_ROOT/bundle-link/docs/perf/evidence/current/note.json"
link_digest="$(python3 - <<'PY'
import hashlib
from pathlib import Path
# digest of symlink target contents if read follows link — promoter must reject symlink first
print("0"*64)
PY
)"
link_allow="$CASE_ROOT/allowlist-link.json"
python3 - "$link_allow" "$S" "$link_digest" <<'PY'
import json, sys
path, source, digest = sys.argv[1:]
json.dump(
    {
        "expected_source": source,
        "campaign": "storage",
        "paths": ["docs/perf/evidence/current/note.json"],
        "historical_paths": [],
        "digests": {"docs/perf/evidence/current/note.json": digest},
    },
    open(path, "w", encoding="utf-8"),
)
PY
expect_fail symlink bash "$SCRIPT_DIR/promote-governed-evidence.sh" \
  --source-root "$repo" --expected-source "$S" --expected-remote origin --expected-ref HEAD \
  --campaign storage --bundle-root "$CASE_ROOT/bundle-link" --allowlist "$link_allow" \
  --promotion-root "$CASE_ROOT/promo-link"

# Wrong campaign
wrong_campaign="$CASE_ROOT/allowlist-campaign.json"
python3 - "$wrong_campaign" "$S" "$digest" <<'PY'
import json, sys
path, source, digest = sys.argv[1:]
json.dump(
    {
        "expected_source": source,
        "campaign": "other",
        "paths": ["docs/perf/evidence/current/note.json"],
        "historical_paths": [],
        "digests": {"docs/perf/evidence/current/note.json": digest},
    },
    open(path, "w", encoding="utf-8"),
)
PY
# restore clean bundle file for digest match path existence
printf 'new-current-evidence\n' >"$bundle/docs/perf/evidence/current/note.json"
# remove historical pollution from earlier test
rm -f "$bundle/docs/perf/evidence/historical.txt"
expect_fail campaign bash "$SCRIPT_DIR/promote-governed-evidence.sh" \
  --source-root "$repo" --expected-source "$S" --expected-remote origin --expected-ref HEAD \
  --campaign storage --bundle-root "$bundle" --allowlist "$wrong_campaign" \
  --promotion-root "$CASE_ROOT/promo-campaign"

echo "promote-governed-evidence-test: PASS"
