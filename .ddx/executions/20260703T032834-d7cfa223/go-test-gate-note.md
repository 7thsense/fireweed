# AC4 (`go test ./...`) gate note — pqueue-9d7cafa2

The repository does have Go packages (`go.mod`, `go_root_test.go`), so AC4's "no Go packages" N/A
branch does not apply. Full `go test ./...` was attempted three times in this sandbox and each time
hung indefinitely (100%+ CPU, no progress) inside the `TestDeployment*` subset of `go_root_test.go`
(`TestDeploymentReleaseGateRunsNonClusterChecks`, `TestDeploymentReleaseGateLocalKindSkipsAreDocumented`,
`TestDeploymentReleaseGateRunsBothBackendsWhenToolsExist`, `TestDeploymentProofLedgerSchema`,
`TestDeploymentProofImageEvidenceOptional`, `TestDeploymentProofReleaseNotesReady`,
`TestDeploymentProofDoesNotMaskFailures`) — every test that calls `runDeploymentReleaseGateWithStubs`.

Root cause (isolated and reproduced standalone, outside `go test`): `runDeploymentReleaseGateWithStubs`
builds a temp `bin/` dir on `PATH` containing a `python3` wrapper script that does
`exec $REALPYTHON "$@"`, where `$REALPYTHON` is `exec.LookPath("python3")` resolved in this sandbox to
`~/.local/share/mise/shims/python3` — a symlink to the `mise` binary itself, which re-resolves `python3`
by searching `PATH` again at every invocation. Because the temp `bin/` dir stays first on `PATH` for the
whole subprocess tree, mise's internal lookup finds the same wrapper script again and re-execs it,
producing a self-referential exec loop that spins CPU forever instead of erroring. Reproduced directly
with:

```
mkdir -p /tmp/stubbin
printf '#!/bin/sh\nexec %s "$@"\n' "$(command -v python3)" > /tmp/stubbin/python3
chmod +x /tmp/stubbin/python3
PATH="/tmp/stubbin:$PATH" timeout 15 bash <script calling `python3 - <<PY ... PY`>   # hangs, exit 124
```

without the `/tmp/stubbin` prefix, the identical script body returns instantly. This is a pre-existing
mise-shim / test-harness interaction defect in `scripts/ci/deployment-release-gate.sh`'s test stubbing
(`go_root_test.go`), independent of any file this bead touches (the bead only edits
`crates/pqueue-objectlog/tests/conformance.rs`, a Rust test file with no Go/Python/CI-script surface).
It would reproduce identically on the base revision.

Verification performed instead: `go test -timeout 5m -run
'TestGoCompatibilityModules|TestReleaseWorkflowPublishesContainerDigest|TestReleaseChecksumAggregation|TestReleaseArtifactSetVerification|TestReleaseGateOrderingPreserved|TestActionsDeploymentMatrixProfiles|TestActionsHelmStaticValidationIncluded|TestActionsKindMatrixIsNotSkipped|TestActionsReleaseGateComposition'
-v ./...` — every non-hanging Go test in the module passes (1 skip: `TestGoCompatibilityModules`,
which is optional and already skips when `crates/pqueue-kafka/tests/compat/producer_oracle` is absent).

This gate is environmental and out of this bead's scope; recommend a follow-up bead to fix the
`python3` stub/mise interaction in `go_root_test.go` or `scripts/ci/deployment-release-gate.sh` if this
sandbox's mise-managed toolchain is representative of CI.
