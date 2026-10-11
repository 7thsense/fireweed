# One concrete queue consumer handle

Iteration 29 adds `fireweed::queue::Queue`: a cloneable concrete handle with private
storage. Consumers depend on `fireweed` and name request/result types, Bytes,
Clock/SystemClock/UtcTimestamp, errors, limits and futures through its queue module.
Backend selection occurs only at construction. There is no backend injection/getter,
transaction/session, capability negotiation or optional unsupported operation.

The nine mandatory methods delegate the original store future directly: provision,
insert, filtered priority claim, atomic commit with state/new work, stats, state
read, indexed query, receipt lookup and shutdown. No task, clock, admission or
cancellation layer is added. Clones share native storage/admission; shutdown through
one clone closes admission and drains admitted jobs for all clones.

With the `postgres` feature, Queue::postgres(PostgresOptions, Arc<dyn Clock>) opens
the native asynchronous baseline. Options contain a redacted ConfigSecret DSN, the
common QueueLimits and pool size (default two). SystemClock is the production
logical clock; test clocks use the same interface. This is separate from retired
legacy PostgreSQL log/projection axes and does not restore them on the old facade.

```rust,no_run
use fireweed::queue::{ConfigSecret, PostgresOptions, Queue, QueueError, SystemClock};
use std::sync::Arc;

async fn baseline(dsn: String) -> Result<Queue, QueueError> {
    Queue::postgres(PostgresOptions::new(ConfigSecret::new(dsn)), Arc::new(SystemClock)).await
}
```

The explicitly registered public-only PostgreSQL fixture imports no core/engine/
backend types. It runs every shared contract scenario: ordinary atomic settlement,
competing claims, expiry/admin controls, ordered long values and cursors, typed
predicate/order oracles, independent groups/replay/sparse stats, ordered insert
uniqueness and shrinking upserts, indexed additional attributes, numeric buckets,
long-name response bounds, acknowledged state/work/receipt replay after reopen.
It compares the exact original receipt across reopen and checks shared shutdown
through a clone. The identical S3/Turso fixture must eventually construct Queue
through its own factory and run these same calls/assertions.

This lands the facade and native binding only. S3/Turso construction, bounded broad
administrative whole-operation wire/preview/size outcomes, Snorri/Cayce migration,
consumer JSON model/visibility bindings and matched release qualification remain
open. The legacy public facade remains until those cutover gates are fulfilled.
