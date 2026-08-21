# Adversarial review: writer-contention recovery round 18

Review the current plan against round 17 and source; do not implement.

Round 17 was folded by:

- splitting pre-position cancellable append failure from any post-`engine.produce`
  error, including `advance_high_water`, which is ambiguous and must poison,
  never cancel or reuse positions;
- wrapping the actual `produce_immediate` path (`engine.produce` plus periodic
  `advance_high_water`→`put_json`) in S3p's Fireweed deadline and dropping the
  unrelated create-only retry claim;
- testing short injected timeout separately from the asserted 30 s default;
- splitting S3f across memory/sqlite/postgres async products and S3v across
  Turso/request-id/AC-TXN-4, then making S3c depend on both; read-side poison
  assertions live only in S3c;
- putting exact coverage inside `claimed_targets`, covering Renew, Reassign, and
  Finalize callers;
- deleting step 4's conflicting 500 ms wait bound: S3m bounds wait, while 500 ms
  begins only for post-coverage select/admit/encode work;
- making the N=100k shadow hold through a real representative packed append
  publication and naming the ignored command/evidence path;
- allowing a correctness-required S3c rate rebaseline when exact coverage costs
  more than 10%, while preserving absolute T2 and comparing S5 to that baseline;
- declaring the Claim/non-Claim admission caps intentionally additive (2,048
  maximum at defaults).

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. `Convergence: YES` requires no BLOCKING and no WARNING.
