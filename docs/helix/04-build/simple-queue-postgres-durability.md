# Native PostgreSQL durability qualification

The baseline uses the mandatory shared queue contract and acknowledged atomic
state/work/receipt oracle. Native test bindings change infrastructure setup and
fault injection, never weaken that oracle.

Writer-process loss is covered by queue_process_loss: after acknowledgement, the
writer is killed without store shutdown and a fresh verifier process checks the
same receipt, completed input, state version/value, emitted work and exact replay.

Iteration 28 adds queue_server_crash. It exclusively creates a new cluster under
this checkout's target directory using an explicit FIREWEED_API007_PG_BIN. It uses
an ephemeral localhost port, no Unix sockets, and checks SHOW data_directory against
its owned canonical directory before mutation and after recovery. No existing URL
or data directory is accepted. fsync, full_page_writes and synchronous_commit are
explicitly on and checked. The test awaits the shared acknowledged atomic group,
then stops only its own server through pg_ctl immediate shutdown without store
shutdown or a requested checkpoint. It restarts the same data directory, requires
interrupted-system and automatic-recovery log evidence, and reruns the identical
shared state/work/receipt/replay oracle. Final server stop and owned-directory
removal are required; the guard also stops the private server during unwinding.

[PostgreSQL 16's shutdown documentation](https://www.postgresql.org/docs/16/server-shutdown.html)
describes immediate SIGQUIT shutdown as terminating children and exiting without
normal database shutdown processing, followed by WAL recovery on restart. This
fixture demonstrates acknowledged-transaction recovery after that immediate
shutdown. It does not simulate storage power loss, lying fsync hardware, filesystem
corruption, SIGKILL of all OS processes or sustained overload during a crash.

Verified locally against stock PostgreSQL 16.15 with the shared API-007 semantics.
Run the isolated fixture independently of hosted/shared PostgreSQL services:

```sh
FIREWEED_API007_PG_BIN=/absolute/path/to/postgresql/bin \
  cargo test -p fireweed-postgres --test queue_server_crash \
  -- --ignored --nocapture
```

S3/Turso must bind the same acknowledged-effect assertions to its own log/apply
crash and exact-genesis reconstruction fault scenarios. This PostgreSQL result
does not qualify that path or replace matched useful-work performance benchmarks.
