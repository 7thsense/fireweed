# Astra Ultra review — iteration 28

Model: gpt-6-astra, Ultra. Base: a5af1102.

APPROVE. The new queue_server_crash fixture exclusively creates a private cluster,
uses bounded pg_ctl calls with its owned data directory, verifies actual cluster
identity and full durable settings, then abruptly stops PostgreSQL after the
shared atomic-group acknowledgement and before writer shutdown. The same cluster
restarts with interrupted-system/WAL recovery log evidence; a fresh native store
runs the same retained receipt, state, source/emitted work and exact replay oracle.
Final stop/removal are required. The guard stops on unwinding and retains/reports
its directory if shutdown cannot be confirmed. English evidence is pinned to C.

Review strengthened the common acknowledged-loss oracle to compare the hydrated
state version with its acknowledged receipt entry and verify the indexed run
field, in addition to the existing value and replay assertions. Both loss fixtures
use these identical assertions. Native documentation now records the narrow
immediate-shutdown recovery proof and retains the remaining qualification gates.

Verification: new private-cluster test passes on stock PostgreSQL 16.15; writer
subprocess kill/fresh verifier also passes the strengthened shared oracle; strict
crash-test and conformance-library Clippy, formatting and independent diff checks
pass. The initial settings-check runtime conflict was corrected to an async client.

This proves acknowledged-transaction recovery after PostgreSQL immediate shutdown,
not storage power loss, corruption, host-wide SIGKILL or S3/Turso durability.
