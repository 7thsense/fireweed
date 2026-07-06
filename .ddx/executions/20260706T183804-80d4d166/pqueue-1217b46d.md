# pqueue-1217b46d Execution Report

## Changes
- Switched the postgres composition root to `BlockingBackend::from_arc(Arc::new(backend))` so the composed backend goes through the Arc-based constructor seam.
- Added a compile-time regression test that typechecks `BlockingBackend::from_arc` against `pqueue_postgres::ComposedPostgresBackend`.

## Verification
- `cargo test -p pqueue-server --test postgres_native --features postgres,tls`
- `go test ./...`
- `lefthook run pre-commit`

## Notes
- `lefthook run pre-commit` exited successfully but reported that no Lefthook config files are present in this worktree, so the hook was a no-op.
