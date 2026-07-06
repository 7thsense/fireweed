# Execution Report

- Bead: `pqueue-46ea00f4`
- Scope: relational claim-shape regressions for SQLite and Postgres

## Change

- Extended `claimed_item_shape_whole_cohort_omits_per_item_lease_token` so each cohort member carries a distinct `fields` map.
- Added assertions that whole-cohort claims retain each member's current `fields` map while still omitting per-item `lease_token`.

## Verification

- `cargo test -p pqueue-sqlite --test relational_conformance update_fields -- --nocapture`
- `cargo test -p pqueue-sqlite --test relational_conformance claimed_item_shape_whole_cohort_omits_per_item_lease_token -- --nocapture`
- `cargo test -p pqueue-postgres --test relational_conformance update_fields -- --nocapture`
- `cargo test -p pqueue-postgres --test relational_conformance claimed_item_shape_whole_cohort_omits_per_item_lease_token -- --nocapture`
- `go test ./...`

## Notes

- Postgres relational tests were env-gated and skipped cleanly because `PQUEUE_PG_TEST_URL` was not set in this workspace.
