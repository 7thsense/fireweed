# AC4: Lefthook Pre-Commit Gate

**Result**: GATE FAILURE — operator_required.

**Details**: `lefthook` tool is installed at `/home/linuxbrew/.linuxbrew/bin/lefthook` but no lefthook config file exists in the repository. No `lefthook.yml`, `.lefthook.yml`, or `.config/lefthook/` found. No git `pre-commit` hook exists at `.git/hooks/pre-commit`.

**Evidence**: Running `lefthook run pre-commit` produces: "No config files with names ['lefthook' '.lefthook' '.config/lefthook'] have been found."

**Operator action required**: Install lefthook configuration to enable pre-commit hook gating.
