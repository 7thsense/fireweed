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
