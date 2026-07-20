#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/perf/resp_readiness.sh
source "${ROOT}/scripts/perf/resp_readiness.sh"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
calls=0
sleep() { :; }

# shellcheck disable=SC2329 # Called indirectly by wait_for_resp_integer.
resp() {
  local output="$1"
  calls=$((calls + 1))
  if ((calls == 1)); then return 75; fi
  printf ':4\r\n' >"${output}"
}

wait_for_resp_integer "${work}/ready.resp" 3 XLEN t1:q1
[[ "${calls}" == 2 ]] || { echo "transport failure was not retried" >&2; exit 1; }

calls=0
# shellcheck disable=SC2329 # Called indirectly by wait_for_resp_integer.
resp() { calls=$((calls + 1)); return 23; }
status=0
wait_for_resp_integer "${work}/semantic.resp" 3 XLEN t1:q1 || status=$?
[[ "${status}" == 23 ]] || { echo "semantic failure was retried or masked" >&2; exit 1; }
[[ "${calls}" == 1 ]] || { echo "semantic failure called resp more than once" >&2; exit 1; }

for semantic_frame in '-ERR not ready' '-MOVED 1 127.0.0.1:8080'; do
  calls=0
  # shellcheck disable=SC2329 # Called indirectly by wait_for_resp_integer.
  resp() { calls=$((calls + 1)); printf '%s\r\n' "${semantic_frame}" >"$1"; }
  status=0
  wait_for_resp_integer "${work}/error.resp" 3 XLEN t1:q1 || status=$?
  [[ "${status}" != 0 && "${status}" != 75 ]] || { echo "RESP error was retryable" >&2; exit 1; }
  [[ "${calls}" == 1 ]] || { echo "RESP error was retried" >&2; exit 1; }
done

calls=0
# shellcheck disable=SC2329 # Called indirectly by wait_for_resp_integer.
resp() { calls=$((calls + 1)); return 65; }
status=0
wait_for_resp_integer "${work}/invalid.resp" 3 XLEN t1:q1 || status=$?
[[ "${status}" == 65 ]] || { echo "protocol error status was masked" >&2; exit 1; }
[[ "${calls}" == 1 ]] || { echo "protocol error was retried" >&2; exit 1; }
