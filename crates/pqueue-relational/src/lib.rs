//! Driver-neutral relational projection contract.
//!
//! This crate owns schema text, row/value codecs, and SQL constants that must remain identical across
//! relational drivers. It deliberately contains no database client or connection abstraction.

mod codec;
mod row;
mod schema;
mod sql;

pub use codec::*;
pub use row::*;
pub use schema::*;
pub use sql::*;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use pqueue_core::{ItemId, ItemState, UtcTimestamp};

    use super::*;

    #[test]
    fn owned_projection_tables_are_declared_by_the_schema() {
        for table in OWNED_PROJECTION_TABLES {
            assert!(
                RELATIONAL_SCHEMA.contains(&format!("CREATE TABLE IF NOT EXISTS {table}")),
                "owned table {table} is absent from the relational schema"
            );
        }
    }

    #[test]
    fn driver_neutral_value_codecs_round_trip() {
        let fields = BTreeMap::from([
            ("empty".to_string(), Bytes::new()),
            ("opaque".to_string(), Bytes::from_static(&[0, 1, 255])),
        ]);
        assert_eq!(
            fields_from_json(fields_to_json(&fields).expect("encode fields"))
                .expect("decode fields"),
            fields
        );

        let timestamp = UtcTimestamp::new(-1, 999_999_999).expect("timestamp");
        assert_eq!(nanos_ts(ts_nanos(timestamp)), timestamp);
        for state in [
            ItemState::Pending,
            ItemState::Leased,
            ItemState::Complete,
            ItemState::Failed,
        ] {
            assert_eq!(parse_state(state_str(state)).expect("decode state"), state);
        }
    }

    #[test]
    fn claim_by_query_replay_item_ids_ignore_other_response_fields() {
        let item = ItemId::mint(1, 2, 3);
        let raw = serde_json::json!({
            "item_ids": [item],
            "lease_token": "opaque-token",
            "worker_id": null,
        })
        .to_string();
        assert_eq!(
            claim_by_query_replay_item_ids(&raw).expect("decode replay"),
            vec![item]
        );
    }
}
