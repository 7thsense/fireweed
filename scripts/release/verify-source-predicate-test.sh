#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CASE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/fireweed-source-predicate.XXXXXX")"
cleanup() { rm -rf "$CASE_ROOT"; }
trap cleanup EXIT
fail() { echo "verify-source-predicate-test: $*" >&2; exit 1; }

repo="$CASE_ROOT/repo"
mkdir -p "$repo"
git -C "$CASE_ROOT" init -q repo
git -C "$repo" config user.name Test
git -C "$repo" config user.email test@example.invalid
git -C "$repo" remote add origin "https://example.invalid/fireweed.git"
printf '%s\n' 'target/' '.ddx/agent-logs/' 'node_modules/' '__pycache__/' 'examples/python-resp/.venv/' >"$repo/.gitignore"
printf 'ok\n' >"$repo/README.md"
git -C "$repo" add .
git -C "$repo" commit -qm seed
S="$(git -C "$repo" rev-parse HEAD)"

bash "$SCRIPT_DIR/verify-source-predicate.sh" \
  --mode source --source-root "$repo" --expected-source "$S" \
  --expected-remote origin --expected-ref HEAD

# Wrong source
if bash "$SCRIPT_DIR/verify-source-predicate.sh" \
  --mode source --source-root "$repo" --expected-source "0000000000000000000000000000000000000000" \
  --expected-remote origin --expected-ref HEAD >/dev/null 2>&1; then
  fail "wrong expected-source accepted"
fi

# Dirty tracked product path
printf 'dirty\n' >>"$repo/README.md"
if bash "$SCRIPT_DIR/verify-source-predicate.sh" \
  --mode source --source-root "$repo" --expected-source "$S" \
  --expected-remote origin --expected-ref HEAD >/dev/null 2>&1; then
  fail "dirty product path accepted"
fi
git -C "$repo" checkout -- README.md

# Forbidden credential path
printf 'secret\n' >"$repo/.env.garage-e3"
if bash "$SCRIPT_DIR/verify-source-predicate.sh" \
  --mode source --source-root "$repo" --expected-source "$S" \
  --expected-remote origin --expected-ref HEAD >/dev/null 2>&1; then
  fail "forbidden .env.garage-e3 accepted"
fi
rm -f "$repo/.env.garage-e3"

# Raw untracked product path
printf 'loose\n' >"$repo/loose.txt"
if bash "$SCRIPT_DIR/verify-source-predicate.sh" \
  --mode source --source-root "$repo" --expected-source "$S" \
  --expected-remote origin --expected-ref HEAD >/dev/null 2>&1; then
  fail "raw untracked product path accepted"
fi
rm -f "$repo/loose.txt"

# Remote mismatch
if bash "$SCRIPT_DIR/verify-source-predicate.sh" \
  --mode source --source-root "$repo" --expected-source "$S" \
  --expected-remote "https://evil.example/fireweed.git" --expected-ref HEAD >/dev/null 2>&1; then
  fail "remote mismatch accepted"
fi

# E-mode dual root
promo="$CASE_ROOT/promoted"
mkdir -p "$promo"
printf 'evidence\n' >"$promo/row.json"
allow="$CASE_ROOT/allow.json"
python3 - "$allow" "$S" <<'PY'
import json, sys
json.dump({"expected_source": sys.argv[2], "paths": ["row.json"]}, open(sys.argv[1], "w"))
PY
bash "$SCRIPT_DIR/verify-source-predicate.sh" \
  --mode e --source-root "$repo" --expected-source "$S" \
  --expected-remote origin --expected-ref HEAD \
  --promoted-root "$promo" --promoted-allowlist "$allow"

# E-mode requires distinct roots
if bash "$SCRIPT_DIR/verify-source-predicate.sh" \
  --mode e --source-root "$repo" --expected-source "$S" \
  --expected-remote origin --expected-ref HEAD \
  --promoted-root "$repo" --promoted-allowlist "$allow" >/dev/null 2>&1; then
  fail "identical dual roots accepted"
fi

# Local/global exclude masking must NOT hide raw untracked product paths.
# Tracked .gitignore rules alone are policy authority (P0/P17a).
mkdir -p "$repo/.git/info"
printf 'masked-by-exclude.txt\n' >"$repo/.git/info/exclude"
printf 'secret-product\n' >"$repo/masked-by-exclude.txt"
if bash "$SCRIPT_DIR/verify-source-predicate.sh" \
  --mode source --source-root "$repo" --expected-source "$S" \
  --expected-remote origin --expected-ref HEAD >/dev/null 2>&1; then
  fail "info/exclude-masked untracked product path accepted"
fi
rm -f "$repo/masked-by-exclude.txt" "$repo/.git/info/exclude"

# Tracked dirty path outside .ddx fails; dirty only under .ddx is inventoried, not product-dirty.
mkdir -p "$repo/.ddx"
printf 'operator\n' >"$repo/.ddx/operator-note.txt"
# untracked .ddx is inventory-only; product tree must still pass
bash "$SCRIPT_DIR/verify-source-predicate.sh" \
  --mode source --source-root "$repo" --expected-source "$S" \
  --expected-remote origin --expected-ref HEAD \
  | tee "$CASE_ROOT/ddx-inventory.out" >/dev/null
grep -Eq 'ddx_untracked_count=[1-9]' "$CASE_ROOT/ddx-inventory.out" ||
  fail "untracked .ddx inventory not reported"
rm -rf "$repo/.ddx"

# Tracked product dirty outside .ddx still fails after .ddx inventory path.
printf 'x\n' >"$repo/extra-product.txt"
git -C "$repo" add extra-product.txt
git -C "$repo" commit -qm extra
S2="$(git -C "$repo" rev-parse HEAD)"
printf 'dirty-tracked\n' >>"$repo/extra-product.txt"
if bash "$SCRIPT_DIR/verify-source-predicate.sh" \
  --mode source --source-root "$repo" --expected-source "$S2" \
  --expected-remote origin --expected-ref HEAD >/dev/null 2>&1; then
  fail "tracked-outside-.ddx dirty path accepted"
fi
git -C "$repo" checkout -- extra-product.txt

echo "verify-source-predicate-test: PASS"
