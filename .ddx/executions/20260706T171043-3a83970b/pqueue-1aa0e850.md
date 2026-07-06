# pqueue-1aa0e850 execution report

Validated:

- `cargo test -p pqueue-resp --test e2e TestKafkaConsumerGroupsStayBrokerOwnedByHeimq -- --nocapture`
- `cargo fmt --all --check`
- `lefthook run pre-commit`
- `lefthook run pre-push`

Notes:

- `lefthook run pre-commit` and `lefthook run pre-push` reported that no Lefthook config files were present in this worktree.
- The test asserts that `XGROUP`/`XINFO GROUPS` do not persist named consumer-group state in pqueue and that `XINFO STREAM` still reports `last-delivered-id = 0-0` after group activity.
