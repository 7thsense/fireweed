#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

python3 - <<'PY'
from pathlib import Path
import tomllib

repo_root = Path.cwd()
config_path = repo_root / ".cargo" / "config.toml"
config_text = config_path.read_text()
config = tomllib.loads(config_text)

assert config.get("build", {}).get("jobs") == 4, "[build].jobs must equal 4"
linux = config.get("target", {}).get("x86_64-unknown-linux-gnu", {})
assert linux.get("linker") == ".cargo/fireweed-linker.sh", (
    "the Linux GNU target must use the tracked portable linker wrapper"
)

checked_paths = [
    config_path,
    repo_root / ".cargo" / "fireweed-linker.sh",
]
for path in checked_paths:
    text = path.read_text()
    assert "/home/linuxbrew" not in text, f"absolute Homebrew path in {path}"

docs = "\n".join(
    path.read_text()
    for path in (repo_root / "README.md", repo_root / "CONTRIBUTING.md")
    if path.exists()
)
for required in ("4 build jobs", "CARGO_BUILD_JOBS", "falls back", "mold"):
    assert required in docs, f"development docs must mention {required!r}"
PY

wrapper="${repo_root}/.cargo/fireweed-linker.sh"
test -x "${wrapper}"

stub_root="$(mktemp -d)"
trap 'rm -rf "${stub_root}"' EXIT

available_bin="${stub_root}/available"
fallback_bin="${stub_root}/fallback"
mkdir -p "${available_bin}" "${fallback_bin}"

for directory in "${available_bin}" "${fallback_bin}"; do
    printf '%s\n' \
        '#!/usr/bin/bash' \
        'printf "cc\\n" > "${FIREWEED_LINKER_TRACE}"' \
        'printf "%s\\n" "$@" >> "${FIREWEED_LINKER_TRACE}"' \
        'exit "${FIREWEED_LINKER_STUB_EXIT:-0}"' \
        > "${directory}/cc"
    chmod +x "${directory}/cc"
done

printf '%s\n' \
    '#!/usr/bin/bash' \
    'printf "clang\\n" > "${FIREWEED_LINKER_TRACE}"' \
    'printf "%s\\n" "$@" >> "${FIREWEED_LINKER_TRACE}"' \
    'exit "${FIREWEED_LINKER_STUB_EXIT:-0}"' \
    > "${available_bin}/clang"
printf '%s\n' '#!/usr/bin/bash' 'exit 0' > "${available_bin}/mold"
chmod +x "${available_bin}/clang" "${available_bin}/mold"

available_trace="${stub_root}/available.trace"
env -i \
    PATH="${available_bin}" \
    FIREWEED_LINKER_TRACE="${available_trace}" \
    /usr/bin/bash "${wrapper}" alpha 'two words'

mapfile -t available_lines < "${available_trace}"
test "${available_lines[0]}" = "clang"
test "${available_lines[1]}" = "-fuse-ld=mold"
test "${available_lines[2]}" = "alpha"
test "${available_lines[3]}" = "two words"

fallback_trace="${stub_root}/fallback.trace"
env -i \
    PATH="${fallback_bin}" \
    FIREWEED_LINKER_TRACE="${fallback_trace}" \
    /usr/bin/bash "${wrapper}" beta 'three words'

mapfile -t fallback_lines < "${fallback_trace}"
test "${fallback_lines[0]}" = "cc"
test "${fallback_lines[1]}" = "beta"
test "${fallback_lines[2]}" = "three words"

set +e
env -i \
    PATH="${available_bin}" \
    FIREWEED_LINKER_TRACE="${available_trace}" \
    FIREWEED_LINKER_STUB_EXIT=23 \
    /usr/bin/bash "${wrapper}" gamma
wrapper_status=$?
set -e
test "${wrapper_status}" -eq 23

printf 'cargo host safeguards verified\n'
