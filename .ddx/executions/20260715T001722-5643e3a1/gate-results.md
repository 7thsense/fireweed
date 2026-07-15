# Gate Results — pqueue-1b1fb5ec

## TestObjectlogHeadCasReviewCorpusGoGate

**Result:** NOT-APPLICABLE
**Command:** `go test ./...`
**Output:** `pattern ./...: directory prefix . does not contain main module or its selected dependencies`
**Rationale:** No `go.mod` file exists anywhere in the repository. The project is a Rust workspace, not a Go project. Per AC5, this gate is recorded as not-applicable.

## TestObjectlogHeadCasReviewCorpusLefthookGate

**Result:** OPERATOR_REQUIRED
**Command:** `lefthook run pre-commit`
**Output:** `No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found`
**Rationale:** Lefthook is installed (v2.1.10) but no lefthook configuration file exists in the repository. Per AC6, missing lefthook config is recorded as an operator_required gate failure. An operator must create a lefthook configuration before this gate can pass.
