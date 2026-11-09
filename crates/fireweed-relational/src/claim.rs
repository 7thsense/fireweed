//! Carrier for one claimed row read from the relational serving tables.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassSClaimedItem {
    pub item_id: String,
    pub client_item_key: String,
    pub payload: Option<Vec<u8>>,
    pub item_version: i64,
    pub retry_count: i64,
    pub lease_expires_at: i64,
    pub priority: Option<String>,
    pub group_key: Option<String>,
    pub not_before: Option<i64>,
    pub fields_json: String,
    pub metadata_json: String,
    pub max_attempts: i64,
    pub entity_document: Option<String>,
    pub index_fields: Option<Vec<u8>>,
    pub gate_keys: Vec<String>,
}
