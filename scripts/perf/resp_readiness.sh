#!/usr/bin/env bash

# Retry only classified transport failures and well-framed not-ready responses.
# Any other helper failure is semantic and is returned immediately.
wait_for_resp_integer() {
  local output="$1" attempts="$2"
  shift 2
  local attempt status
  for ((attempt = 0; attempt < attempts; attempt += 1)); do
    if resp "${output}" "$@"; then
      if grep -Eq '^:[0-9]+' "${output}"; then return 0; fi
    else
      status=$?
      if ((status != 75)); then return "${status}"; fi
    fi
    sleep 1
  done
  return 70
}
