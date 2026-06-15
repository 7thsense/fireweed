// Retention GC helpers (TD-002 §Retention and Compaction).
//
// INV-5: GC must not delete any record whose replay or audit window has not yet
// expired. `expire_terminal_items` intentionally never touches `pqueue_commands`;
// command-log GC is a separate, bounded operation that must gate on ALL associated
// windows (idempotency, terminal, replay, audit) having independently elapsed.

use time::OffsetDateTime;

/// Delete expired request-idempotency records (expires_at <= cutoff).
///
/// Safe to call concurrently; Postgres DELETE is atomic per row.
pub async fn expire_request_idempotency(
    client: &tokio_postgres::Client,
    cutoff: OffsetDateTime,
) -> Result<u64, tokio_postgres::Error> {
    client
        .execute(
            "DELETE FROM pqueue_request_idempotency WHERE expires_at <= $1",
            &[&cutoff],
        )
        .await
}

/// Delete expired item-key convergence records (expires_at <= cutoff).
pub async fn expire_item_key_retention(
    client: &tokio_postgres::Client,
    cutoff: OffsetDateTime,
) -> Result<u64, tokio_postgres::Error> {
    client
        .execute(
            "DELETE FROM pqueue_item_key_retention WHERE expires_at <= $1",
            &[&cutoff],
        )
        .await
}

/// Delete terminal items (complete/failed) whose terminal_at <= cutoff.
///
/// Scoped to one (tenant_id, queue_id) pair so the caller can page across
/// queues with a bounded shared worker.
///
/// INV-5 safety: this function NEVER touches `pqueue_commands`. Command-log
/// rows must outlive terminal item rows; a separate, gated command-log GC
/// operation is responsible for verifying that all idempotency, terminal,
/// replay, and audit windows have independently elapsed before deleting any
/// command row.
pub async fn expire_terminal_items(
    client: &tokio_postgres::Client,
    tenant_id: &str,
    queue_id: &str,
    cutoff: OffsetDateTime,
) -> Result<u64, tokio_postgres::Error> {
    client
        .execute(
            "DELETE FROM pqueue_items
             WHERE tenant_id = $1
               AND queue_id   = $2
               AND lifecycle_state IN ('complete', 'failed')
               AND terminal_at <= $3",
            &[&tenant_id, &queue_id, &cutoff],
        )
        .await
}
