# Iteration 84: admitted genesis-bound projection checkpoint

The stock-Turso projector now exposes an internal checkpoint read consuming an
operation-owned FWQL2 genesis record. Complete framing, same-job ownership and
semantic definition checks precede the native query. A single SQL statement
binds full tenant/name, exact original raw definition and cursor secret while
returning only bounded integer/eight-byte progress fields. Lazy CASE gates stored
definition/secret SQL type and length before evaluating equality, so malformed
large blobs cannot trigger unbounded comparison scratch. Stored epoch cannot
precede the supplied genesis epoch. Stored definition or
secret payloads are not returned or cloned. Missing projection yields None;
mismatched authority or invalid zero/type/length progress fails closed.

A whole consuming bounded blocking worker retains input and output admission.
The result retains its operation reservation. This is a projection snapshot,
not durable log coverage, a live fence, or a public consumer transaction.
Recovery must still validate every record against a freshly acquired log target,
compare original genesis and prefix sequence at the checkpoint, and apply the
suffix before serving consumers. No QueueStore/backend qualification is implied.

Physical tests cover exact genesis binding, wrong secret, oversized definition/secret and malformed
progress, zero cursor/epoch and cold reopen. Eleven projector tests passed;
strict stock-Turso local all-target Clippy and diff hygiene are required before
landing. Logical admission does not bound Turso internal page-cache/RSS.
