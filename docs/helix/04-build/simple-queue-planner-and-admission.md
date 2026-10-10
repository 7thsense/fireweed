---
ddx:
  id: build-simple-queue-planner-admission
  depends_on:
    - api-queue-claim-commit-contract
    - build-simple-queue-binding-v1
  status: accepted
---

# Shared atomic planner and owned admission, iteration 3

`queue_planner` implements lease/token/version checks, state CAS, claimed
completion/retry/release/renewal, claimless state/work changes, new work and
non-reused per-key versions as one deterministic group plan. SQL stores its delta
and receipt atomically; S3 stores the same semantic result in the authoritative
command path. A rejected group leaves the loaded snapshot unchanged and supplies
its durable rejection outcome. Ordered groups apply against prior successes.

Storage loads only addressed records and their persistent version high waters,
including tombstones; it protects that snapshot until the complete group delta
is durably decided. The planner is not an I/O transaction manager and does not
make an unprotected read/check/write safe. It requires prior envelope validation
and durable request-identity resolution. Existing log ownership/fencing remains
independent. Claim/query selection stays storage-indexed, not a snapshot scan.

`queue_admission` supplies one reservation for an owned storage job, atomically
checking close, slot and byte saturation. Growing charged loaded/planning buffers
must occur before retention. Caller cancellation cannot own/drop that reservation
once the job is admitted; the backend retains it through its terminal outcome.
Growth failure after admission is OutcomeUnknown, never Busy/known rollback;
backend must retain/resolve owned work and not discard published groups. Loaded
version and sequence inconsistencies fail closed before planning. Closing stops new admission but admitted jobs may finish. Adapters still must
implement async completion/drain and charge actual buffers conservatively.

Tests cover state+new-work+claim atomicity, late failure leaving all candidate
writes unchanged, lease expiry before reclaim, shared-state sibling independence,
ABA after state deletion/reinsert, past/future RetryAt age and max-attempts
exhaustion (following main, not Rearm semantics), forbidden lease theft, reservation transfer
across close, byte saturation and failed-growth charge conservation. These are
semantic unit tests; neither backend is activated or claimed qualified yet.
