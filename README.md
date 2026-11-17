# Fireweed

Fireweed is the data plane for Snorri and Cayce: queues and tables for
short-lived, scale-out workflow data, changed through a small typed DSL and
run as plain SQL.

A consumer describes its data in a **schema** document: queues, tables,
typed fields, indexes and named reads. It changes that data with
**programs**: lists of typed statements (`insert`, `claim`, `settle`,
`renew`, `update`, `delete`, `assert`, `select`, `read`, `pause`,
`resume`) that run as one transaction. Both are YAML or JSON.

An executor creates real tables and indexes from the schema and runs each
statement as one SQL statement over them. The database chooses rows by
index, orders writers with row locks and decides with its clock when work
is due. The executor keeps nothing in the process between calls.

- [ADR-027](docs/helix/02-design/adr/ADR-027-data-plane-interface-is-the-implementation-boundary.md):
  the decision and the owner's rulings.
- [API-008](docs/helix/02-design/contracts/API-008-data-plane-dsl.md): the
  DSL reference and the PostgreSQL executor's rules.

## Crates

| Crate | What it is |
|-------|------------|
| `fireweed-dsl` | Schema and program documents, and the checker that resolves a program against a schema. |
| `fireweed-dsl-postgres` | The PostgreSQL 18 executor: `apply_schema`, `get_schema`, `execute`, `execute_batch` and `sweep`. |

## Example

A schema:

```yaml
version: 1
queues:
  work:
    max_attempts: 5
    fields:
      campaign: {type: text, index: true}
```

A program that enqueues two items, and one that claims them:

```yaml
request_id: enqueue-1
issued_at: 2026-11-16T09:30:00Z
statements:
  - insert: {into: work, rows: [{key: a, campaign: c1}, {key: b, campaign: c1}]}
```

```yaml
request_id: claim-1
issued_at: 2026-11-16T09:30:05Z
statements:
  - claim: {from: work, where: {eq: [campaign, c1]}, limit: 10, lease: 30s, worker: w1}
```

The claim returns each item's `id` and lease `token`. A later program
settles them:

```yaml
request_id: settle-1
issued_at: 2026-11-16T09:30:09Z
statements:
  - settle: {queue: work, outcome: complete, items: [{id: 1, token: "<token>"}]}
```

From Rust:

```rust
let schema: fireweed_dsl::Schema = fireweed_dsl::from_yaml(SCHEMA)?;
fireweed_dsl_postgres::apply_schema(&mut client, "tenant", "space", &schema, None).await?;
let program: fireweed_dsl::Program = fireweed_dsl::from_yaml(ENQUEUE)?;
let response = fireweed_dsl_postgres::execute(&mut client, "tenant", "space", &program).await?;
```

## Development

Prerequisites: [rustup](https://rustup.rs/) (the repository selects Rust
1.99.0) and PostgreSQL 18. `docker compose up -d postgres` starts one on
port 55432.

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
FIREWEED_PG_TEST_URL=postgres://fireweed:fireweed@127.0.0.1:55432/fireweed cargo test --workspace
cargo deny check
```

Without `FIREWEED_PG_TEST_URL` the executor's tests skip.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Contributions are issues only; see
[CONTRIBUTING.md](CONTRIBUTING.md).
