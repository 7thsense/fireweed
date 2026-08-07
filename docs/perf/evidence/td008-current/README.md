# Current TD-008 evidence (P11)

Historical `docs/perf/evidence/td008-terminal-reap-frontier.jsonl` is immutable
and must not be rewritten. It does not qualify current product evidence.

## Semantic current ID

`CURRENT-TD008-DELIVERY-MATRIX`

## Producer

```sh
cargo test -p fireweed-release --test td008_evidence -- --nocapture
```

Assertions:

- Observed run-owned ledger rows only (static attestation rejected)
- Tracked historical path rejects write/delete authority
- Observed marker must match the live run

Promotion of allowlisted current paths is P18-owned.
