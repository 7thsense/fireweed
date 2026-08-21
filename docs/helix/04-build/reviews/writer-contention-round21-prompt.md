# Adversarial review: writer-contention recovery round 21

Review the current plan against round 20 and source; do not implement.

Round 20 was folded by:

- adding S3r, a three-file universal Turso render/validation slice used by
  atomic, object-log, and S5 public response paths; deferred mode uses one
  committed snapshot, fallback uses one joined autocommit statement;
- asserting effective pragmas for the existing uncommitted shared reader and
  every committed pooled reader;
- adding a new coherent IMMEDIATE validate/apply transaction for commit_validate,
  with coverage failure occurring retryably before per-entry outcomes;
- defining caps per port: Renew uses min(5 s, requested new expiry-now), while
  Reassign/Finalize/strict transition use fixed 5 s because they expose no
  deadline;
- splitting S3c coverage from S3r isolation and wiring B3r into S3m/S5;
- changing KeyedQueueGate to count active+queued globally, so all three 1,024
  admission classes have the same basis and the 3,072 maximum is real;
- using a bounded eight-connection committed selection pool with pool-wait
  metric, returning snapshots before append;
- adding RecoveryOnly for pre-serving legacy outbox drain;
- replacing gate-first terminology with SelectionFenceAdmission-first.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. `Convergence: YES` requires no BLOCKING and no WARNING.
