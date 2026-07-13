# Baseline gate evidence for `pqueue-00e8b0cd`

Scope: capture the Rust workspace baseline gates separately from objectlog release-specific validation.

Toolchain note:
- `rustup toolchain list` shows `1.92.0-x86_64-unknown-linux-gnu` is installed and active.
- The shell's direct `cargo +1.92.0 ...` invocation was not available, so the pinned toolchain was executed with `rustup run 1.92.0 cargo ...`.

## 1. Workspace format baseline

Command:

```text
rustup run 1.92.0 cargo fmt --all --check
```

Result:

- Exit status: `0`
- Outcome: passed
- Output: none

## 2. Workspace clippy baseline

Command:

```text
rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings
```

Result:

- Exit status: `101`
- Outcome: failed
- Failure evidence:
  - `crates/pqueue-objectlog/src/segmented.rs:1213:13`
  - `error[E0308]: mismatched types`
  - `crates/pqueue-objectlog/src/segmented.rs:2157:14`
  - `error: unused variable: candidates`

## 3. Go workspace gate

Command:

```text
go test ./...
```

Result:

- Exit status: `1`
- Outcome: not applicable for this worktree
- Evidence:
  - `pattern ./...: directory prefix . does not contain main module or its selected dependencies`

## 4. Lefthook pre-commit gate

Command:

```text
lefthook run pre-commit
```

Result:

- Exit status: `1`
- Outcome: operator_required gate failure
- Evidence:
  - `No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "<repo root>"`

