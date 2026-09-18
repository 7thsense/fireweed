#!/usr/bin/env bash
# Old CLI entrypoints must fail before any build, provider access, or evidence write.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
SCRATCH=$(mktemp -d)
trap 'rm -rf "$SCRATCH"' EXIT
mkdir -p "$SCRATCH/bin" "$SCRATCH/evidence"
for command in cargo docker curl git; do
  cat >"$SCRATCH/bin/$command" <<'BLOCKED'
#!/usr/bin/env bash
printf '%s\n' "unexpected external command: $0" >>"$FIREWEED_E3_TEST_EVENTS"
exit 99
BLOCKED
  chmod +x "$SCRATCH/bin/$command"
done

for entrypoint in tp002-e3-s3.sh tp002-e3-minio.sh run-e3-minio-durable.sh; do
  set +e
  PATH="$SCRATCH/bin:$PATH" \
  FIREWEED_E3_TEST_EVENTS="$SCRATCH/events" \
  FIREWEED_E3_EVIDENCE_DIR="$SCRATCH/evidence" \
  FIREWEED_S3_TEST_SECRET_KEY=retirement-fixture-secret \
    bash "$REPO_ROOT/scripts/perf/$entrypoint" >"$SCRATCH/output" 2>&1
  status=$?
  set -e
  if [[ "$status" != 2 ]]; then
    echo "$entrypoint: expected retirement status 2, got $status" >&2
    cat "$SCRATCH/output" >&2
    exit 1
  fi
  grep -Fq 'E3 live qualification is retired' "$SCRATCH/output"
  grep -Fq 'cannot produce new E3 qualification' "$SCRATCH/output"
  if grep -Fq retirement-fixture-secret "$SCRATCH/output"; then
    echo "$entrypoint: exposed the supplied secret" >&2
    exit 1
  fi
  test ! -e "$SCRATCH/events"
  if [[ -n "$(find "$SCRATCH/evidence" -mindepth 1 -print -quit)" ]]; then
    echo "$entrypoint: emitted evidence for a retired producer" >&2
    exit 1
  fi
done
