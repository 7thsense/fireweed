# Helm static validation gate — execution evidence (pqueue-d923bbdf)

## Summary

Added a deterministic, cluster-free static validation gate for the pqueue Helm
chart: `scripts/ci/helm-gate.sh`. It runs `helm lint --strict`, `helm template`,
and `kubeconform` schema validation for both supported backend profiles, and is
wired into GitHub Actions as a lightweight `helm` job that does not require kind.

## Files

- `scripts/ci/helm-gate.sh` — the gate (new, executable).
- `charts/pqueue/ci/postgres-native-values.yaml` — CI values, `postgres_native`.
- `charts/pqueue/ci/object-log-sqlite-projection-values.yaml` — CI values,
  `object_log_sqlite_projection`.
- `docs/deployment/helm-static-validation.md` — prerequisites and pinned-version
  documentation.
- `.github/workflows/ci.yml` — additive `helm` job (install Helm + run gate).

## Acceptance criteria

1. **`bash scripts/ci/helm-gate.sh` passes from a clean checkout.** Verified with
   an empty cache (`rm -rf target/helm-gate`) and `kubeconform` absent from
   `PATH`: the gate auto-installed kubeconform and exited `0`.
2. **Runs `helm lint` and templates both profiles.** The gate iterates
   `postgres_native` and `object_log_sqlite_projection`, running `helm lint
   --strict` then `helm template` for each. Observed kubeconform summaries:
   `postgres_native` → 4 resources valid; `object_log_sqlite_projection` →
   5 resources valid (adds the SQLite projection PVC).
3. **Validates rendered manifests with a deterministic validator; pinned/
   documented version.** Uses `kubeconform` pinned to `v0.6.7` against Kubernetes
   API schema `v1.31.0`. Auto-install downloads the pinned release tarball and
   verifies its SHA-256 against an embedded checksum table (linux/darwin ×
   amd64/arm64). Versions are documented in
   `docs/deployment/helm-static-validation.md`.
4. **Existing CI entrypoints still pass.** Changes are purely additive:
   `scripts/ci/pr-gate.sh` and `scripts/ci/release-gate.sh` are unmodified
   (`git diff --name-only` lists only `.github/workflows/ci.yml`, a new isolated
   `helm` job). All `scripts/ci/*.sh` pass `bash -n`. A full local run of the
   enforcing pr-gate/release-gate is out of scope per "as appropriate for local
   cost": those gates require sibling checkouts (fjord, object-log, heimq), the
   1.92.0 + nightly Rust toolchains, coverage tooling, and release-scale product
   validation, none of which this isolated worktree provides; their behavior is
   unchanged by construction.

## Notes

- Auto-installed binaries cache under `target/helm-gate/bin/` (git-ignored).
- An existing `kubeconform` on `PATH` is used as-is (local toolchains respected).
- kind cluster creation and runtime smoke tests are intentionally out of scope.
