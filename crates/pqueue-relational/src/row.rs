use pqueue_core::{ItemId, OrderField, QueryFilter, TypedValue};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RangeScanCursorState {
    pub index: String,
    pub filters: Vec<QueryFilter>,
    pub order_by: Vec<OrderField>,
    pub anchor_item_id: ItemId,
    pub anchor_values: Vec<TypedValue>,
    /// Canonical declared-index key used for an index-backed keyset seek.
    #[serde(default)]
    pub anchor_index_key: Option<Vec<u8>>,
}

/// Canonical typed-index keys grouped by item.
pub type TypedIndexRows = Vec<(String, Vec<(String, Vec<u8>)>)>;
