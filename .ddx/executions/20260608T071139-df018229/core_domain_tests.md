# core_domain_tests evidence

Verification command:

```text
cargo test -p pqueue-core core_domain_tests
```

Observed coverage:

- Recurrence and cohort are mutually exclusive at `CreateQueue::validate`.
- `cohort_policy.completion_bound_ms` must be `<= progress_bound_ms`.
- Cohort creation requires `group_co_residency=true`.
- `shard_count` defaults to `1`, respects the deployment cap, and rejects `0`.

Ledger citations:

- `API-001` core queue definition and `CreateQueue` validation contract.
- `API-002` API result/error type surface.
- `ADR-004` group co-residency and recurrence/cohort exclusivity rules.
