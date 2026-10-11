//! Canonical queue representation shared by native relational adapters. Database
//! SQL, physical row IDs, transactions and candidate execution stay in adapters.
//! Callers admit storage buffers before encoding/decoding; these helpers allocate
//! no hidden queue image or backend-specific business planner.
use fireweed_core::queue_contract::{
    CollectionDefinition, Lifecycle, QueueError, QueueResult, QueueSchema,
};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
fn corrupt(error: impl std::fmt::Display) -> QueueError {
    QueueError::OutcomeUnknown(error.to_string())
}
pub fn encode<T: Serialize>(value: &T) -> QueueResult<Vec<u8>> {
    postcard::to_allocvec(value).map_err(corrupt)
}
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> QueueResult<T> {
    let (value, rest) = postcard::take_from_bytes(bytes).map_err(corrupt)?;
    if !rest.is_empty() {
        return Err(corrupt("trailing persisted bytes"));
    }
    Ok(value)
}
/// Discovery only: full values remain authority even when probes collide.
pub fn discovery_digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
pub fn discovery_prefix(bytes: &[u8]) -> Vec<u8> {
    bytes[..bytes.len().min(256)].to_vec()
}
pub fn encode_lifecycle(value: Lifecycle) -> i16 {
    match value {
        Lifecycle::Pending => 0,
        Lifecycle::Leased => 1,
        Lifecycle::Completed => 2,
        Lifecycle::Failed => 3,
    }
}
pub fn decode_lifecycle(value: i16) -> QueueResult<Lifecycle> {
    match value {
        0 => Ok(Lifecycle::Pending),
        1 => Ok(Lifecycle::Leased),
        2 => Ok(Lifecycle::Completed),
        3 => Ok(Lifecycle::Failed),
        _ => Err(corrupt("corrupt lifecycle")),
    }
}
pub fn collection_number(schema: &QueueSchema, name: Option<&str>) -> QueueResult<i32> {
    match name {
        None => Ok(0),
        Some(name) => schema
            .state
            .keys()
            .position(|key| key == name)
            .ok_or_else(|| QueueError::Invalid("undeclared state collection".into()))
            .and_then(|n| {
                i32::try_from(n)
                    .ok()
                    .and_then(|n| n.checked_add(1))
                    .ok_or(QueueError::TooLarge)
            }),
    }
}
pub fn collection_definition(
    schema: &QueueSchema,
    number: i32,
) -> QueueResult<&CollectionDefinition> {
    if number == 0 {
        return Ok(&schema.work);
    }
    let index = number
        .checked_sub(1)
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| corrupt("corrupt collection id"))?;
    schema
        .state
        .values()
        .nth(index)
        .ok_or_else(|| corrupt("corrupt collection id"))
}
pub fn field_number(definition: &CollectionDefinition, name: &str) -> QueueResult<i32> {
    if let Some(n) = definition.fields.keys().position(|key| key == name) {
        return i32::try_from(n).map_err(|_| QueueError::TooLarge);
    }
    if definition.admits_field(name) {
        Ok(-1)
    } else {
        Err(QueueError::Invalid("undeclared field".into()))
    }
}
pub fn index_number(definition: &CollectionDefinition, name: &str) -> QueueResult<i32> {
    definition
        .indexes
        .keys()
        .position(|key| key == name)
        .ok_or_else(|| QueueError::Invalid("undeclared index".into()))
        .and_then(|n| i32::try_from(n).map_err(|_| QueueError::TooLarge))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn persisted_ordinals_and_lifecycle_tags_remain_literal() {
        use fireweed_core::queue_contract::FieldType;
        let definition = CollectionDefinition {
            fields: [
                ("z".into(), FieldType::Text),
                ("a".into(), FieldType::Integer),
            ]
            .into(),
            indexes: [
                ("z".into(), vec!["z".into()]),
                ("a".into(), vec!["a".into()]),
            ]
            .into(),
            unique_indexes: Default::default(),
            additional_field_prefixes: ["meta/".into()].into(),
        };
        let schema = QueueSchema {
            work: definition.clone(),
            state: [
                ("z".into(), definition.clone()),
                ("a".into(), definition.clone()),
            ]
            .into(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        };
        schema.validate().unwrap();
        assert_eq!(collection_number(&schema, None).unwrap(), 0);
        assert_eq!(collection_number(&schema, Some("a")).unwrap(), 1);
        assert_eq!(collection_number(&schema, Some("z")).unwrap(), 2);
        assert_eq!(field_number(&definition, "a").unwrap(), 0);
        assert_eq!(field_number(&definition, "z").unwrap(), 1);
        assert_eq!(field_number(&definition, "meta/value").unwrap(), -1);
        assert!(field_number(&definition, "other").is_err());
        assert_eq!(index_number(&definition, "a").unwrap(), 0);
        assert_eq!(index_number(&definition, "z").unwrap(), 1);
        for (value, tag) in [
            (Lifecycle::Pending, 0),
            (Lifecycle::Leased, 1),
            (Lifecycle::Completed, 2),
            (Lifecycle::Failed, 3),
        ] {
            assert_eq!(encode_lifecycle(value), tag);
            assert_eq!(decode_lifecycle(tag).unwrap(), value);
        }
        assert!(decode_lifecycle(-1).is_err());
        assert!(decode_lifecycle(4).is_err());
    }
    #[test]
    fn corrupt_storage_cannot_be_accepted_as_a_valid_prefix_or_negative_collection() {
        let schema = QueueSchema {
            work: CollectionDefinition {
                fields: Default::default(),
                indexes: Default::default(),
                unique_indexes: Default::default(),
                additional_field_prefixes: Default::default(),
            },
            state: Default::default(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        };
        for number in [i32::MIN, -1, 1, i32::MAX] {
            assert!(matches!(
                collection_definition(&schema, number),
                Err(QueueError::OutcomeUnknown(_))
            ));
        }
        let raw = encode(&u64::MAX).unwrap();
        assert_eq!(decode::<u64>(&raw).unwrap(), u64::MAX);
        let mut extra = raw;
        extra.push(0);
        assert!(matches!(
            decode::<u64>(&extra),
            Err(QueueError::OutcomeUnknown(_))
        ));
    }
}
