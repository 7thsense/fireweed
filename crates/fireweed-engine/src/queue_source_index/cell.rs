//! Source fact values. The enclosing map authenticates the full semantic key;
//! these values never infer durable state from a projection's expiry/rows.
use super::QueueSourceSpan;
use crate::{
    queue_group_state_authority::{BodyDescriptor, WorkMetadata},
    queue_log_record::CheckedQueueLogRecord,
    queue_original_authority::OriginalDescriptor,
    queue_schema_codec::DefinitionPin,
};
use fireweed_core::queue_contract::{QueueError, QueueResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Decode borrowing before any admitted identity clone. Shared by source
/// preimage loading and publication validation; both use the full tuple domain.
pub(super) fn unique_owner<'a>(
    bytes: &'a [u8],
    collection: Option<&str>,
) -> QueueResult<crate::queue_publication::IdentityRef<'a>> {
    #[derive(Serialize, Deserialize)]
    struct Owner<'a> {
        #[serde(borrow)]
        collection: Option<&'a str>,
        key: &'a str,
    }
    let (owner, rest): (Owner<'a>, _) = postcard::take_from_bytes(bytes)
        .map_err(|_| QueueError::OutcomeUnknown("invalid source unique owner".into()))?;
    if !rest.is_empty()
        || owner.collection != collection
        || owner.key.trim().is_empty()
        || fireweed_core::queue_contract::encoded_len(&owner)? != bytes.len() as u64
    {
        return Err(QueueError::OutcomeUnknown(
            "invalid source unique owner identity".into(),
        ));
    }
    Ok(crate::queue_publication::IdentityRef {
        collection: owner.collection,
        key: owner.key,
    })
}

/// Isolated projection bytes require their own digest. The span's full-record
/// checksum is used only for complete checked source records on a cache miss.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueSourcePayload {
    span: QueueSourceSpan,
    sha256: [u8; 32],
}
impl QueueSourcePayload {
    pub fn from_checked(
        record: &CheckedQueueLogRecord<'_>,
        offset: u64,
        bytes: &[u8],
    ) -> QueueResult<Self> {
        Ok(Self {
            span: QueueSourceSpan::from_checked(record, offset, bytes)?,
            sha256: Sha256::digest(bytes).into(),
        })
    }
    pub fn span(&self) -> QueueSourceSpan {
        self.span
    }
    pub fn descriptor(&self) -> BodyDescriptor {
        BodyDescriptor {
            bytes: self.span.byte_length(),
            sha256: self.sha256,
        }
    }
    pub fn verify_cached(&self, bytes: &[u8]) -> QueueResult<()> {
        if bytes.len() as u64 != self.span.byte_length()
            || <[u8; 32]>::from(Sha256::digest(bytes)) != self.sha256
        {
            return Err(QueueError::OutcomeUnknown(
                "cached payload differs from authenticated source".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct QueueSourceGenesis {
    pub source: QueueSourceSpan,
    pub definition_pin: DefinitionPin,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum QueueSourceLive {
    Work {
        metadata: WorkMetadata,
        body: QueueSourcePayload,
    },
    State {
        value: QueueSourcePayload,
    },
}
#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct QueueSourceRecord {
    /// Retained after deletion, including publication HWM jumps.
    pub highwater: u64,
    /// Complete membership count; complete field ranges must agree before a
    /// replacement/delete removes postings. Ordinary Preserve need not load it.
    pub field_count: u64,
    pub live: Option<QueueSourceLive>,
}
impl QueueSourceRecord {
    pub fn validate(&self, collection: Option<&str>) -> QueueResult<()> {
        let valid = self.highwater > 0
            && match self.live {
                None => self.field_count == 0,
                Some(QueueSourceLive::State { .. }) => collection.is_some(),
                Some(QueueSourceLive::Work { metadata, .. }) => {
                    collection.is_none()
                        && metadata.version == self.highwater
                        && metadata.insertion_sequence > 0
                        && ((metadata.lifecycle
                            == fireweed_core::queue_contract::Lifecycle::Leased)
                            == metadata.lease_expires_at.is_some())
                        && (metadata.lease_expires_at.is_some() == metadata.lease_hash.is_some())
                }
            };
        if valid {
            Ok(())
        } else {
            Err(QueueError::OutcomeUnknown(
                "invalid source record fact".into(),
            ))
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueSourceOriginal {
    pub fingerprint_format: u8,
    pub fingerprint: [u8; 32],
    pub outcome: QueueSourcePayload,
}
impl QueueSourceOriginal {
    pub fn validate(&self, domain: u8) -> QueueResult<()> {
        if domain <= 2
            && (self.fingerprint_format == 3 || (domain == 2 && self.fingerprint_format == 4))
        {
            Ok(())
        } else {
            Err(QueueError::OutcomeUnknown(
                "invalid source original format/domain".into(),
            ))
        }
    }
    pub fn descriptor(&self) -> OriginalDescriptor {
        let payload = self.outcome.descriptor();
        OriginalDescriptor {
            fingerprint_format: self.fingerprint_format,
            fingerprint: self.fingerprint,
            outcome_bytes: payload.bytes,
            outcome_sha256: payload.sha256,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        queue_admission::QueueAdmission,
        queue_log_record::encode_checked_genesis,
        queue_schema_codec::{AdmittedQueueDefinition, encode_definition},
    };
    use fireweed_core::queue_contract::{CollectionDefinition, QueueAddress, QueueSchema};
    use std::sync::Arc;
    #[test]
    fn isolated_cache_payloads_need_their_own_length_and_digest() {
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
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
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap();
        let record = encode_checked_genesis(
            1,
            &QueueAddress {
                tenant: "t".into(),
                queue: "q".into(),
            },
            &definition,
            &[7; 32],
            4096,
            &job,
        )
        .unwrap();
        let checked = record.checked().unwrap();
        let crate::queue_log_record::QueueLogPayload::Genesis(genesis) = checked.view().payload()
        else {
            panic!()
        };
        let payload =
            QueueSourcePayload::from_checked(&checked, 0, genesis.definition_bytes()).unwrap();
        payload.verify_cached(genesis.definition_bytes()).unwrap();
        let mut changed = genesis.definition_bytes().to_vec();
        changed[0] ^= 1;
        assert!(payload.verify_cached(&changed).is_err());
        assert!(
            payload
                .verify_cached(&changed[..changed.len() - 1])
                .is_err()
        );
        assert_eq!(
            payload.span().resolve(&checked, 0).unwrap(),
            genesis.definition_bytes()
        );
        let original = QueueSourceOriginal {
            fingerprint_format: 4,
            fingerprint: [9; 32],
            outcome: payload,
        };
        assert!(original.validate(2).is_ok());
        assert!(original.validate(0).is_err());
        assert!(original.validate(1).is_err());
        assert!(original.validate(3).is_err());
        assert_eq!(
            original.descriptor().outcome_bytes,
            genesis.definition_bytes().len() as u64
        );
    }
}
