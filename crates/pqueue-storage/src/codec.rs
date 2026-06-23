//! Stable byte serialization for `CommandEnvelope`, shared by durable backends.
//!
//! The domain command types deliberately do not derive serde (their id newtypes
//! validate on construction), so this module owns the DTO mapping and round-trips
//! an envelope through JSON. Backends that persist the command log (object-log,
//! sqlite) encode with [`encode_envelope`] and decode with [`decode_envelope`].
//!
//! `CreateQueue` is control-plane state and is not encoded here.

use pqueue_core::{
    ClientItemKey, DecimalValue, IdentifierError, ItemId, PriorityValue, QueueId, RequestId,
    TenantId, UtcTimestamp,
};

use crate::commands::{
    BatchClaimCommand, BatchFinalizeCommand, BatchPushCommand, BatchRenewLeasesCommand,
    BatchUpdateCommand, CohortExpiredCommand, CommandEnvelope, CommandId, FinalizeKind,
    FinalizeOutcome, LeaseExpiredCommand, PurgeItemsCommand, PushItem, QueueCommand,
};
use crate::types::{CommandChecksum, ShardId};

/// Error encoding or decoding a command envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecError(pub String);

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "command codec error: {}", self.0)
    }
}

impl std::error::Error for CodecError {}

/// Serialize a `CommandEnvelope` to bytes for durable storage.
pub fn encode_envelope(envelope: &CommandEnvelope) -> Result<Vec<u8>, CodecError> {
    serde_json::to_vec(&envelope_to_dto(envelope)?)
        .map_err(|err| CodecError(format!("serialize: {err}")))
}

/// Deserialize a `CommandEnvelope` from bytes written by [`encode_envelope`].
pub fn decode_envelope(payload: &[u8]) -> Result<CommandEnvelope, CodecError> {
    let dto: EnvelopeDto =
        serde_json::from_slice(payload).map_err(|err| CodecError(format!("deserialize: {err}")))?;
    dto_to_envelope(dto)
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct EnvelopeDto {
    command_id: String,
    request_id: Option<String>,
    tenant_id: String,
    queue_id: String,
    shard_id: u32,
    item_ids: Vec<String>,
    checksum: u32,
    created_at: TimestampDto,
    command: CommandDto,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "body")]
enum CommandDto {
    BatchPush {
        items: Vec<PushItemDto>,
    },
    BatchUpdate {
        item_ids: Vec<String>,
    },
    BatchClaim {
        item_ids: Vec<String>,
        lease_token: String,
        lease_expires_at: TimestampDto,
    },
    BatchRenewLeases {
        item_ids: Vec<String>,
        lease_expires_at: TimestampDto,
    },
    BatchFinalize {
        outcomes: Vec<FinalizeOutcomeDto>,
    },
    LeaseExpired {
        item_ids: Vec<String>,
    },
    CohortExpired {
        group_key: String,
    },
    PurgeItems {
        item_ids: Vec<String>,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PushItemDto {
    client_item_key: String,
    item_id: String,
    priority: Option<PriorityDto>,
    not_before: Option<TimestampDto>,
    max_attempts: u32,
    payload: Option<Vec<u8>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "value")]
enum PriorityDto {
    Timestamp(TimestampDto),
    Int64(i64),
    Decimal { mantissa: i128, scale: u32 },
    Text(String),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct FinalizeOutcomeDto {
    item_id: String,
    kind: FinalizeKindDto,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum FinalizeKindDto {
    Complete,
    Fail,
    Retry,
    Release,
    Rearm,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TimestampDto {
    seconds: i64,
    nanoseconds: u32,
}

fn envelope_to_dto(envelope: &CommandEnvelope) -> Result<EnvelopeDto, CodecError> {
    Ok(EnvelopeDto {
        command_id: envelope.command_id.0.clone(),
        request_id: envelope
            .request_id
            .as_ref()
            .map(|id| id.as_str().to_string()),
        tenant_id: envelope.tenant_id.as_str().to_string(),
        queue_id: envelope.queue_id.as_str().to_string(),
        shard_id: envelope.shard_id.as_u32(),
        item_ids: envelope
            .item_ids
            .iter()
            .map(|id| id.as_str().to_string())
            .collect(),
        checksum: envelope.checksum.0,
        created_at: timestamp_to_dto(&envelope.created_at),
        command: command_to_dto(&envelope.command)?,
    })
}

fn command_to_dto(command: &QueueCommand) -> Result<CommandDto, CodecError> {
    match command {
        QueueCommand::CreateQueue(_) => Err(CodecError(
            "CreateQueue is control-plane state and is not encoded in the command log".to_string(),
        )),
        QueueCommand::BatchPush(command) => Ok(CommandDto::BatchPush {
            items: command.items.iter().map(push_item_to_dto).collect(),
        }),
        QueueCommand::BatchUpdate(command) => Ok(CommandDto::BatchUpdate {
            item_ids: ids_to_strings(&command.item_ids),
        }),
        QueueCommand::BatchClaim(command) => Ok(CommandDto::BatchClaim {
            item_ids: ids_to_strings(&command.item_ids),
            lease_token: command.lease_token.clone(),
            lease_expires_at: timestamp_to_dto(&command.lease_expires_at),
        }),
        QueueCommand::BatchRenewLeases(command) => Ok(CommandDto::BatchRenewLeases {
            item_ids: ids_to_strings(&command.item_ids),
            lease_expires_at: timestamp_to_dto(&command.lease_expires_at),
        }),
        QueueCommand::BatchFinalize(command) => Ok(CommandDto::BatchFinalize {
            outcomes: command
                .outcomes
                .iter()
                .map(|outcome| FinalizeOutcomeDto {
                    item_id: outcome.item_id.as_str().to_string(),
                    kind: finalize_kind_to_dto(outcome.kind),
                })
                .collect(),
        }),
        QueueCommand::LeaseExpired(command) => Ok(CommandDto::LeaseExpired {
            item_ids: ids_to_strings(&command.item_ids),
        }),
        QueueCommand::CohortExpired(command) => Ok(CommandDto::CohortExpired {
            group_key: command.group_key.clone(),
        }),
        QueueCommand::PurgeItems(command) => Ok(CommandDto::PurgeItems {
            item_ids: ids_to_strings(&command.item_ids),
        }),
    }
}

fn ids_to_strings(ids: &[ItemId]) -> Vec<String> {
    ids.iter().map(|id| id.as_str().to_string()).collect()
}

fn push_item_to_dto(item: &PushItem) -> PushItemDto {
    PushItemDto {
        client_item_key: item.client_item_key.as_str().to_string(),
        item_id: item.item_id.as_str().to_string(),
        priority: item.priority.as_ref().map(priority_to_dto),
        not_before: item.not_before.as_ref().map(timestamp_to_dto),
        max_attempts: item.max_attempts,
        payload: item.payload.as_ref().map(|payload| payload.to_vec()),
    }
}

fn priority_to_dto(priority: &PriorityValue) -> PriorityDto {
    match priority {
        PriorityValue::Timestamp(value) => PriorityDto::Timestamp(timestamp_to_dto(value)),
        PriorityValue::Int64(value) => PriorityDto::Int64(*value),
        PriorityValue::Decimal(value) => PriorityDto::Decimal {
            mantissa: value.mantissa,
            scale: value.scale,
        },
        PriorityValue::Text(value) => PriorityDto::Text(value.clone()),
    }
}

fn timestamp_to_dto(value: &UtcTimestamp) -> TimestampDto {
    TimestampDto {
        seconds: value.seconds,
        nanoseconds: value.nanoseconds,
    }
}

fn finalize_kind_to_dto(kind: FinalizeKind) -> FinalizeKindDto {
    match kind {
        FinalizeKind::Complete => FinalizeKindDto::Complete,
        FinalizeKind::Fail => FinalizeKindDto::Fail,
        FinalizeKind::Retry => FinalizeKindDto::Retry,
        FinalizeKind::Release => FinalizeKindDto::Release,
        FinalizeKind::Rearm => FinalizeKindDto::Rearm,
    }
}

fn dto_to_envelope(dto: EnvelopeDto) -> Result<CommandEnvelope, CodecError> {
    let command = dto_to_command(dto.command)?;
    Ok(CommandEnvelope {
        command_id: CommandId(dto.command_id),
        request_id: dto
            .request_id
            .map(RequestId::new)
            .transpose()
            .map_err(id_err)?,
        tenant_id: TenantId::new(dto.tenant_id).map_err(id_err)?,
        queue_id: QueueId::new(dto.queue_id).map_err(id_err)?,
        shard_id: ShardId::new(dto.shard_id),
        item_ids: dto
            .item_ids
            .into_iter()
            .map(ItemId::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(id_err)?,
        command,
        checksum: CommandChecksum(dto.checksum),
        created_at: dto_to_timestamp(dto.created_at)?,
    })
}

fn dto_to_command(dto: CommandDto) -> Result<QueueCommand, CodecError> {
    match dto {
        CommandDto::BatchPush { items } => Ok(QueueCommand::BatchPush(BatchPushCommand {
            items: items
                .into_iter()
                .map(dto_to_push_item)
                .collect::<Result<Vec<_>, _>>()?,
        })),
        CommandDto::BatchUpdate { item_ids } => Ok(QueueCommand::BatchUpdate(BatchUpdateCommand {
            item_ids: strings_to_ids(item_ids)?,
        })),
        CommandDto::BatchClaim {
            item_ids,
            lease_token,
            lease_expires_at,
        } => Ok(QueueCommand::BatchClaim(BatchClaimCommand {
            item_ids: strings_to_ids(item_ids)?,
            lease_token,
            lease_expires_at: dto_to_timestamp(lease_expires_at)?,
        })),
        CommandDto::BatchRenewLeases {
            item_ids,
            lease_expires_at,
        } => Ok(QueueCommand::BatchRenewLeases(BatchRenewLeasesCommand {
            item_ids: strings_to_ids(item_ids)?,
            lease_expires_at: dto_to_timestamp(lease_expires_at)?,
        })),
        CommandDto::BatchFinalize { outcomes } => {
            Ok(QueueCommand::BatchFinalize(BatchFinalizeCommand {
                outcomes: outcomes
                    .into_iter()
                    .map(|outcome| {
                        Ok(FinalizeOutcome {
                            item_id: ItemId::new(outcome.item_id).map_err(id_err)?,
                            kind: dto_to_finalize_kind(outcome.kind),
                        })
                    })
                    .collect::<Result<Vec<_>, CodecError>>()?,
            }))
        }
        CommandDto::LeaseExpired { item_ids } => {
            Ok(QueueCommand::LeaseExpired(LeaseExpiredCommand {
                item_ids: strings_to_ids(item_ids)?,
            }))
        }
        CommandDto::CohortExpired { group_key } => {
            Ok(QueueCommand::CohortExpired(CohortExpiredCommand {
                group_key,
            }))
        }
        CommandDto::PurgeItems { item_ids } => Ok(QueueCommand::PurgeItems(PurgeItemsCommand {
            item_ids: strings_to_ids(item_ids)?,
        })),
    }
}

fn dto_to_push_item(dto: PushItemDto) -> Result<PushItem, CodecError> {
    Ok(PushItem {
        client_item_key: ClientItemKey::new(dto.client_item_key).map_err(id_err)?,
        item_id: ItemId::new(dto.item_id).map_err(id_err)?,
        priority: dto.priority.map(dto_to_priority).transpose()?,
        not_before: dto.not_before.map(dto_to_timestamp).transpose()?,
        max_attempts: dto.max_attempts,
        payload: dto.payload.map(bytes::Bytes::from),
    })
}

fn dto_to_priority(dto: PriorityDto) -> Result<PriorityValue, CodecError> {
    Ok(match dto {
        PriorityDto::Timestamp(value) => PriorityValue::Timestamp(dto_to_timestamp(value)?),
        PriorityDto::Int64(value) => PriorityValue::Int64(value),
        PriorityDto::Decimal { mantissa, scale } => {
            PriorityValue::Decimal(DecimalValue { mantissa, scale })
        }
        PriorityDto::Text(value) => PriorityValue::Text(value),
    })
}

fn strings_to_ids(ids: Vec<String>) -> Result<Vec<ItemId>, CodecError> {
    ids.into_iter()
        .map(ItemId::new)
        .collect::<Result<Vec<_>, _>>()
        .map_err(id_err)
}

fn dto_to_timestamp(dto: TimestampDto) -> Result<UtcTimestamp, CodecError> {
    UtcTimestamp::new(dto.seconds, dto.nanoseconds).map_err(|err| CodecError(err.to_string()))
}

fn dto_to_finalize_kind(dto: FinalizeKindDto) -> FinalizeKind {
    match dto {
        FinalizeKindDto::Complete => FinalizeKind::Complete,
        FinalizeKindDto::Fail => FinalizeKind::Fail,
        FinalizeKindDto::Retry => FinalizeKind::Retry,
        FinalizeKindDto::Release => FinalizeKind::Release,
        FinalizeKindDto::Rearm => FinalizeKind::Rearm,
    }
}

fn id_err(err: IdentifierError) -> CodecError {
    CodecError(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(s: i64, n: u32) -> UtcTimestamp {
        UtcTimestamp::new(s, n).unwrap()
    }

    fn item(s: &str) -> ItemId {
        ItemId::new(s).unwrap()
    }

    fn envelope(command: QueueCommand) -> CommandEnvelope {
        CommandEnvelope {
            command_id: CommandId::new("cmd"),
            request_id: Some(RequestId::new("req").unwrap()),
            tenant_id: TenantId::new("t").unwrap(),
            queue_id: QueueId::new("q").unwrap(),
            shard_id: ShardId::new(3),
            item_ids: vec![item("a"), item("b")],
            command,
            checksum: CommandChecksum(99),
            created_at: ts(123, 456),
        }
    }

    /// Encoding is stable: decode(encode(x)) re-encodes to the same bytes for
    /// every command variant, every priority kind, and a fully-populated item.
    /// (Avoids needing PartialEq on the domain types.)
    fn assert_round_trips(command: QueueCommand) {
        let env = envelope(command);
        let bytes = encode_envelope(&env).expect("encode");
        let decoded = decode_envelope(&bytes).expect("decode");
        let reencoded = encode_envelope(&decoded).expect("re-encode");
        assert_eq!(bytes, reencoded, "round-trip not stable");
    }

    #[test]
    fn round_trips_all_command_and_priority_variants() {
        let full_push = PushItem {
            client_item_key: ClientItemKey::new("k").unwrap(),
            item_id: item("a"),
            priority: Some(PriorityValue::Decimal(DecimalValue {
                mantissa: -170_141_183_460_469_231_731_687_303_715_884_105_727i128,
                scale: 9,
            })),
            not_before: Some(ts(50, 7)),
            max_attempts: 5,
            payload: Some(bytes::Bytes::from_static(b"\x00\xffbinary")),
        };
        for priority in [
            PriorityValue::Timestamp(ts(1, 0)),
            PriorityValue::Int64(-42),
            PriorityValue::Text("hi".into()),
        ] {
            assert_round_trips(QueueCommand::BatchPush(BatchPushCommand {
                items: vec![PushItem {
                    client_item_key: ClientItemKey::new("k2").unwrap(),
                    item_id: item("c"),
                    priority: Some(priority),
                    not_before: None,
                    max_attempts: 1,
                    payload: None,
                }],
            }));
        }
        assert_round_trips(QueueCommand::BatchPush(BatchPushCommand {
            items: vec![full_push],
        }));
        assert_round_trips(QueueCommand::BatchUpdate(BatchUpdateCommand {
            item_ids: vec![item("a")],
        }));
        assert_round_trips(QueueCommand::BatchClaim(BatchClaimCommand {
            item_ids: vec![item("a")],
            lease_token: "lease".into(),
            lease_expires_at: ts(9, 0),
        }));
        assert_round_trips(QueueCommand::BatchRenewLeases(BatchRenewLeasesCommand {
            item_ids: vec![item("a")],
            lease_expires_at: ts(9, 0),
        }));
        for kind in [
            FinalizeKind::Complete,
            FinalizeKind::Fail,
            FinalizeKind::Retry,
            FinalizeKind::Release,
            FinalizeKind::Rearm,
        ] {
            assert_round_trips(QueueCommand::BatchFinalize(BatchFinalizeCommand {
                outcomes: vec![FinalizeOutcome {
                    item_id: item("a"),
                    kind,
                }],
            }));
        }
        assert_round_trips(QueueCommand::LeaseExpired(LeaseExpiredCommand {
            item_ids: vec![item("a")],
        }));
        assert_round_trips(QueueCommand::CohortExpired(CohortExpiredCommand {
            group_key: "g".into(),
        }));
        assert_round_trips(QueueCommand::PurgeItems(PurgeItemsCommand {
            item_ids: vec![item("a")],
        }));
    }

    #[test]
    fn decimal_i128_mantissa_survives() {
        let env = envelope(QueueCommand::BatchPush(BatchPushCommand {
            items: vec![PushItem {
                client_item_key: ClientItemKey::new("k").unwrap(),
                item_id: item("a"),
                priority: Some(PriorityValue::Decimal(DecimalValue {
                    mantissa: i128::MIN,
                    scale: 0,
                })),
                not_before: None,
                max_attempts: 1,
                payload: None,
            }],
        }));
        let decoded = decode_envelope(&encode_envelope(&env).unwrap()).unwrap();
        match &decoded.command {
            QueueCommand::BatchPush(cmd) => match &cmd.items[0].priority {
                Some(PriorityValue::Decimal(d)) => assert_eq!(d.mantissa, i128::MIN),
                other => panic!("priority not preserved: {other:?}"),
            },
            other => panic!("command not preserved: {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_malformed_payload() {
        assert!(decode_envelope(b"not json").is_err());
    }
}
