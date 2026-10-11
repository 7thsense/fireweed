//! Versioned queue definition persistence. Legacy unframed definitions keep
//! their strict fields/index ordinals; namespace support is an explicit new frame.
use fireweed_core::queue_contract::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
// Ten continuation bytes cannot begin a valid postcard usize/u64 collection
// length, so this marker cannot be mistaken for a valid legacy definition.
const SENTINEL: &[u8; 10] = &[255; 10];
const MAGIC: &[u8; 15] = b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xffFWQD4";
fn unknown(error: impl std::fmt::Display) -> QueueError {
    QueueError::OutcomeUnknown(error.to_string())
}
fn take<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> QueueResult<T> {
    let (value, rest) = postcard::take_from_bytes(bytes).map_err(unknown)?;
    if !rest.is_empty() {
        return Err(unknown("trailing queue definition bytes"));
    }
    Ok(value)
}
#[derive(Serialize, Deserialize)]
struct LegacyDefinition {
    fields: BTreeMap<String, FieldType>,
    indexes: BTreeMap<String, Vec<String>>,
    unique_indexes: BTreeSet<String>,
}
impl From<LegacyDefinition> for CollectionDefinition {
    fn from(value: LegacyDefinition) -> Self {
        Self {
            fields: value.fields,
            indexes: value.indexes,
            unique_indexes: value.unique_indexes,
            additional_field_prefixes: BTreeSet::new(),
        }
    }
}
#[derive(Serialize, Deserialize)]
struct LegacySchema {
    work: LegacyDefinition,
    state: BTreeMap<String, LegacyDefinition>,
    progress_bound_nanos: u64,
    max_attempts: u32,
}
/// Exact persisted-definition identity for resolved publications. Hash the
/// authority's original bytes, including framing; decoding and re-encoding an
/// equivalent legacy schema would bind a different physical definition.
/// This is an identity check, not schema validation or allocation admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefinitionPin([u8; 32]);
impl DefinitionPin {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
    pub fn from_persisted_bytes(raw: &[u8]) -> Self {
        Self(Sha256::digest(raw).into())
    }
    pub fn verify(self, raw: &[u8]) -> QueueResult<()> {
        if self == Self::from_persisted_bytes(raw) {
            Ok(())
        } else {
            Err(unknown("resolved publication definition pin mismatch"))
        }
    }
}

/// Returns only borrowed framing information; measure allocation headers before
/// calling decode_definition on either version.
pub fn definition_payload(raw: &[u8]) -> QueueResult<(bool, &[u8])> {
    if raw.starts_with(SENTINEL) {
        if !raw.starts_with(MAGIC) {
            return Err(unknown("unsupported queue definition frame"));
        }
        Ok((true, &raw[MAGIC.len()..]))
    } else {
        Ok((false, raw))
    }
}
pub fn encode_definition(schema: &QueueSchema) -> QueueResult<Vec<u8>> {
    schema.validate()?;
    postcard::to_extend(schema, MAGIC.to_vec()).map_err(unknown)
}
pub fn decode_definition(raw: &[u8]) -> QueueResult<QueueSchema> {
    let (modern, bytes) = definition_payload(raw)?;
    let schema = if modern {
        take(bytes)?
    } else {
        let old: LegacySchema = take(bytes)?;
        QueueSchema {
            work: old.work.into(),
            state: old
                .state
                .into_iter()
                .map(|(name, def)| (name, def.into()))
                .collect(),
            progress_bound_nanos: old.progress_bound_nanos,
            max_attempts: old.max_attempts,
        }
    };
    schema
        .validate()
        .map_err(|error| unknown(format!("invalid persisted queue schema: {error:?}")))?;
    Ok(schema)
}
pub fn definition_headers(raw: &[u8]) -> QueueResult<u64> {
    let (modern, bytes) = definition_payload(raw)?;
    if modern {
        crate::queue_memory::persisted_schema_headers(bytes)
    } else {
        crate::queue_memory::persisted_legacy_schema_headers(bytes)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_definition_retains_identity_and_modern_namespaces_are_framed() {
        let old = LegacySchema {
            work: LegacyDefinition {
                fields: BTreeMap::from([("kind".into(), FieldType::Text)]),
                indexes: BTreeMap::from([("kind-index".into(), vec!["kind".into()])]),
                unique_indexes: BTreeSet::new(),
            },
            state: BTreeMap::new(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        };
        let raw = postcard::to_allocvec(&old).unwrap();
        let schema = decode_definition(&raw).unwrap();
        assert!(schema.work.additional_field_prefixes.is_empty());
        assert_eq!(
            definition_headers(&raw).unwrap(),
            crate::queue_memory::schema_headers(&schema).unwrap()
        );
        let encoded = encode_definition(&schema).unwrap();
        assert_eq!(decode_definition(&encoded).unwrap(), schema);
        let legacy_pin = DefinitionPin::from_persisted_bytes(&raw);
        let modern_pin = DefinitionPin::from_persisted_bytes(&encoded);
        assert_ne!(legacy_pin, modern_pin);
        legacy_pin.verify(&raw).unwrap();
        modern_pin.verify(&encoded).unwrap();
        assert!(legacy_pin.verify(&encoded).is_err());
        assert!(modern_pin.verify(&raw).is_err());
        let wire_pin = postcard::to_allocvec(&legacy_pin).unwrap();
        assert_eq!(wire_pin.len(), 32);
        assert_eq!(take::<DefinitionPin>(&wire_pin).unwrap(), legacy_pin);
        assert!(definition_payload(&encoded).unwrap().0);
        let mut extra = schema;
        extra
            .work
            .additional_field_prefixes
            .insert("/payload/".into());
        assert_eq!(
            decode_definition(&encode_definition(&extra).unwrap()).unwrap(),
            extra
        );
        extra.state.insert(
            "instances".into(),
            CollectionDefinition {
                fields: BTreeMap::new(),
                indexes: BTreeMap::new(),
                unique_indexes: BTreeSet::new(),
                additional_field_prefixes: BTreeSet::from(["/meta/".into()]),
            },
        );
        assert_eq!(
            definition_headers(&encode_definition(&extra).unwrap()).unwrap(),
            crate::queue_memory::schema_headers(&extra).unwrap()
        );
        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode_definition(&trailing).is_err());
        let mut unsupported = MAGIC.to_vec();
        unsupported[14] = b'5';
        assert!(decode_definition(&unsupported).is_err());
    }
}

/// Complete semantic validation of exact persisted bytes under an owned job.
/// Never reconstruct the pin from an equivalent decoded/re-encoded schema.
pub struct AdmittedQueueDefinition {
    raw: bytes::Bytes,
    schema: QueueSchema,
    pin: DefinitionPin,
    _charge: crate::queue_admission::OwnedQueueBufferCharge,
}
impl AdmittedQueueDefinition {
    pub fn from_raw(
        raw: &[u8],
        job: &std::sync::Arc<crate::queue_admission::QueueReservation>,
    ) -> QueueResult<Self> {
        let headers = definition_headers(raw)?;
        let charge = job.owned_buffer(
            (raw.len() as u64)
                .checked_mul(4)
                .and_then(|n| n.checked_add(headers.checked_mul(4)?))
                .and_then(|n| n.checked_add(1024))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let schema = decode_definition(raw)?;
        let pin = DefinitionPin::from_persisted_bytes(raw);
        Ok(Self {
            raw: bytes::Bytes::copy_from_slice(raw),
            schema,
            pin,
            _charge: charge,
        })
    }
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }
    pub fn schema(&self) -> &QueueSchema {
        &self.schema
    }
    pub fn pin(&self) -> DefinitionPin {
        self.pin
    }
    pub fn belongs_to(&self, job: &crate::queue_admission::QueueReservation) -> bool {
        std::ptr::eq(self._charge.reservation(), job)
    }
}
