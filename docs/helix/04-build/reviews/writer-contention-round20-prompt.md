# Adversarial review: writer-contention recovery round 20

Review the current plan against round 19 and source; do not implement.

Round 19 was folded by:

- replacing the assumed uniform KeyedQueueGate with a compile/test-enforced
  derived Turso admission map: item Claim uses ClaimCoordinator; currently
  ungated typed mutation/grouped-Claim sites use SelectionFenceAdmission;
  existing commit_raw/Reassign gate sites retain KeyedQueueGate; each request
  consumes exactly one class and then the classified fence;
- adding a separate 1,024 SelectionFenceAdmission cap and stating the three
  defaults are intentionally additive (3,072 maximum);
- making claimed-target validation assert a dedicated committed reader, using
  one deferred committed snapshot across item/gate/bearer statements or one
  joined autocommit statement in fallback mode;
- putting coverage before strict transition outcomes, keeping writer
  `commit_validate` coherent inside IMMEDIATE, and making coverage expiry abort
  retryably rather than become a per-entry Rejected result;
- capping claimed-target coverage at the lesser of five seconds and caller
  remaining lease/deadline, gating p99/expiry, and never returning StaleLease
  for coverage expiry;
- aligning the reservation structural gate with pre+post phase budgets plus one
  second slack, with no added linger;
- making pre→post timer cancellation/marking atomic under the packer mutex;
- filing deferred Bpg/S9 for postgres claimed-target parity.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. `Convergence: YES` requires no BLOCKING and no WARNING.
