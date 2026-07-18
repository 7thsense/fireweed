//! SQL shared by relational projection drivers.

/// Read the durable projection cursor for one queue.
pub const SELECT_RELATIONAL_CURSOR: &str =
    "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2";

/// Read materialized item identifiers for counter restoration.
pub const SELECT_MATERIALIZED_ITEM_IDS: &str =
    "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2";
