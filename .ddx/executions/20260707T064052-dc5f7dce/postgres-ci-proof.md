# Postgres CI proof

command: cargo +1.92.0 test -p pqueue-postgres --test conformance push_then_select_eligible_in_priority_order -- --nocapture
environment: PQUEUE_PG_TEST_URL=postgres://postgres:pq@127.0.0.1:5433/postgres
observed_output:
```text
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.10s
     Running tests/conformance.rs (target/debug/deps/conformance-48a24c1a0125a477)

running 1 test
test push_then_select_eligible_in_priority_order ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 40 filtered out; finished in 0.05s

```
