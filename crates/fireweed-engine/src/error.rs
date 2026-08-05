//! Engine error model. The RESP adapter maps these to the canonical `-ERR fireweed ...` replies
//! (TD-006 section 7; asserted verbatim by conformance).

/// Stable stage of a durable-object integrity failure. Adapters carry this
/// enum without parsing display text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableIntegrityStage {
    Manifest,
    Header,
    Bounds,
    RecordCrc32c,
    FrameCrc32c,
    Sha256,
    Payload,
    Position,
}

impl DurableIntegrityStage {
    pub const fn token(self) -> &'static str {
        match self {
            Self::Manifest => "manifest",
            Self::Header => "header",
            Self::Bounds => "bounds",
            Self::RecordCrc32c => "record_crc32c",
            Self::FrameCrc32c => "frame_crc32c",
            Self::Sha256 => "sha256",
            Self::Payload => "payload",
            Self::Position => "position",
        }
    }

    pub fn parse(token: &str) -> Option<Self> {
        Some(match token {
            "manifest" => Self::Manifest,
            "header" => Self::Header,
            "bounds" => Self::Bounds,
            "record_crc32c" => Self::RecordCrc32c,
            "frame_crc32c" => Self::FrameCrc32c,
            "sha256" => Self::Sha256,
            "payload" => Self::Payload,
            "position" => Self::Position,
            _ => return None,
        })
    }
}

impl std::fmt::Display for DurableIntegrityStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.token())
    }
}

/// Errors a port may return. The variant set is the engine's; adapters translate to their wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineError {
    /// Queue (or shard) not found.
    NotFound,
    /// Queue already exists with an incompatible definition.
    QueueDefinitionConflict,
    /// A lifecycle transition is not allowed on the target (e.g. upsert of a claimed item).
    /// Maps to `-ERR fireweed invalid`.
    Invalid(&'static str),
    /// The target item is terminal. Maps to `-ERR fireweed terminal`.
    Terminal,
    /// The lease has been operator-fenced (stale generation). Maps to `-ERR fireweed stale_lease`.
    StaleLease,
    /// The addressed id was superseded by a pending-item replacement. Maps to `-ERR fireweed superseded`.
    Superseded,
    /// The operation is unavailable on this backend's durability class (e.g. upsert on
    /// eventual-apply). Maps to `-ERR fireweed unavailable`.
    Unavailable,
    /// An optimistic-concurrency or cohort conflict. Maps to `-ERR fireweed conflict`.
    Conflict,
    /// A claim/cohort/group unit would exceed `max_items` (API-001 `batch-too-large`).
    /// Maps to `-ERR fireweed batch_too_large`.
    BatchTooLarge,
    /// A retried `request_id` carried a different body (API-001 `request-id-conflict`).
    /// Distinct from the generic `Conflict`. Maps to `-ERR fireweed request_id_conflict`.
    RequestIdConflict,
    /// Intake is blocked by a paused queue. The `drain_intake` flag distinguishes the fully-quiesced
    /// branch-prep mode from the legacy "claims stop, pushes still land" pause.
    Paused { drain_intake: bool },
    /// A `request_id` replay arrived after its retention window (API-001 `request-expired`).
    /// Maps to `-ERR fireweed request_expired`.
    RequestExpired,
    /// An append/claim carried an `expected_epoch` that is not the queue's current durable
    /// `assignment_epoch` — a stale (superseded) owner was fenced (TD-003 Single Authoritative Fencing
    /// Rule, `queue-epoch-stale`). Maps to `-ERR fireweed epoch_stale`.
    EpochFenced,
    /// The principal is not authorized (cross-tenant or missing operator privilege). The RESP
    /// adapter maps this to `-NOPERM` (TD-006 section 2); not an `-ERR fireweed ...` reply.
    Forbidden(&'static str),
    /// The item's entity document violates the queue's compiled entity schema (ADR-011). Rejection
    /// happens before log append, idempotency recording, SQL mutation, or projection apply.
    /// Maps to `-ERR fireweed entity_schema_violation`.
    EntitySchemaViolation(String),
    /// A single request can never fit a configured hard resource limit. This is a permanent invalid request.
    RequestTooLarge { requested: usize, limit: usize },
    /// A bounded internal resource is temporarily exhausted. The caller may retry after load subsides.
    Backpressure { resource: &'static str },
    /// Underlying storage failure (adapter-level).
    Storage(String),
    /// A durable object failed a structured integrity check. The locator is an
    /// opaque queue-scoped token and must never contain an object-store key.
    DurableDataCorrupt {
        stage: DurableIntegrityStage,
        manifest_index: u64,
        locator: String,
    },
    /// Change-record delivery was requested for a Class B (memory-log) cell. Startup-only:
    /// must never escape a mutation or appear in a production commit outcome (TD-008).
    ChangeRecordsRequireDurableLog,
}

impl EngineError {
    /// The canonical `-ERR fireweed ...` token a RESP adapter emits for this error, or `None` for
    /// errors that have a non-`-ERR` mapping (e.g. `NotFound` to nil) handled by the adapter.
    pub fn resp_token(&self) -> Option<&'static str> {
        match self {
            EngineError::Invalid(_) => Some("-ERR fireweed invalid"),
            EngineError::Terminal => Some("-ERR fireweed terminal"),
            EngineError::StaleLease => Some("-ERR fireweed stale_lease"),
            EngineError::Superseded => Some("-ERR fireweed superseded"),
            EngineError::Unavailable => Some("-ERR fireweed unavailable"),
            EngineError::Conflict => Some("-ERR fireweed conflict"),
            EngineError::BatchTooLarge => Some("-ERR fireweed batch_too_large"),
            EngineError::RequestIdConflict => Some("-ERR fireweed request_id_conflict"),
            EngineError::Paused { .. } => Some("-ERR fireweed paused"),
            EngineError::RequestExpired => Some("-ERR fireweed request_expired"),
            EngineError::EpochFenced => Some("-ERR fireweed epoch_stale"),
            EngineError::EntitySchemaViolation(_) => Some("-ERR fireweed entity_schema_violation"),
            EngineError::RequestTooLarge { .. } => Some("-ERR fireweed invalid"),
            EngineError::Backpressure { .. } => Some("-ERR fireweed unavailable"),
            EngineError::QueueDefinitionConflict => Some("-ERR fireweed queue_conflict"),
            EngineError::ChangeRecordsRequireDurableLog => {
                Some("-ERR fireweed change_records_require_durable_log")
            }
            // Forbidden -> `-NOPERM`, NotFound -> nil: non-`-ERR fireweed` mappings handled by the adapter.
            EngineError::NotFound
            | EngineError::Forbidden(_)
            | EngineError::Storage(_)
            | EngineError::DurableDataCorrupt { .. } => None,
        }
    }
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::NotFound => write!(f, "not found"),
            EngineError::QueueDefinitionConflict => write!(f, "queue definition conflict"),
            EngineError::Invalid(why) => write!(f, "invalid: {why}"),
            EngineError::Terminal => write!(f, "terminal"),
            EngineError::StaleLease => write!(f, "stale lease"),
            EngineError::Superseded => write!(f, "superseded"),
            EngineError::Unavailable => write!(f, "unavailable"),
            EngineError::Conflict => write!(f, "conflict"),
            EngineError::BatchTooLarge => write!(f, "batch too large"),
            EngineError::RequestIdConflict => write!(f, "request-id conflict"),
            EngineError::Paused { drain_intake } => {
                write!(f, "paused (drain_intake={drain_intake})")
            }
            EngineError::RequestExpired => write!(f, "request expired"),
            EngineError::EpochFenced => write!(f, "epoch fenced (stale owner)"),
            EngineError::EntitySchemaViolation(msg) => {
                write!(f, "entity schema violation: {msg}")
            }
            EngineError::RequestTooLarge { requested, limit } => {
                write!(f, "request too large: {requested} bytes exceeds {limit}")
            }
            EngineError::Backpressure { resource } => write!(f, "backpressure: {resource}"),
            EngineError::Forbidden(why) => write!(f, "forbidden: {why}"),
            EngineError::Storage(msg) => write!(f, "storage: {msg}"),
            EngineError::DurableDataCorrupt {
                stage,
                manifest_index,
                locator,
            } => write!(
                f,
                "durable data corrupt: stage={stage} manifest_index={manifest_index} locator={locator}"
            ),
            EngineError::ChangeRecordsRequireDurableLog => {
                write!(f, "change records require a durable Class A log")
            }
        }
    }
}

impl std::error::Error for EngineError {}

/// A durable, serializable projection of the [`EngineError`] variants a `commit_transition` entry rejection
/// can carry, so a mixed committed+rejected commit's per-entry outcome can be recorded on the log and
/// replayed BYTE-IDENTICALLY across a restart (bead pqueue-db60657d). Lives next to [`EngineError`] because
/// it mirrors it 1:1: [`CommitRejection::from_error`] projects every variant and
/// [`CommitRejection::into_error`] reconstructs it, so the round-trip preserves `PartialEq` for the errors a
/// commit rejection actually produces.
///
/// The `&'static str`-bearing variants ([`EngineError::Invalid`] / [`EngineError::Forbidden`]) cannot
/// recreate an arbitrary static from a decoded `String`, so [`CommitRejection::into_error`] maps the reasons
/// the commit path actually emits (`commit_validate`'s "item is not leased" and `validate_instance_fence`'s
/// "instance fence is not monotonic") back to their exact literals; any other reason falls back to a stable
/// static. Every rejection `commit_validate` / `validate_instance_fence` / `validate_entity` /
/// `index_validate_push` can emit therefore round-trips byte-identically.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CommitRejection {
    NotFound,
    QueueDefinitionConflict,
    Invalid(String),
    Terminal,
    StaleLease,
    Superseded,
    Unavailable,
    Conflict,
    BatchTooLarge,
    RequestIdConflict,
    Paused {
        drain_intake: bool,
    },
    RequestExpired,
    EpochFenced,
    Forbidden(String),
    EntitySchemaViolation(String),
    RequestTooLarge {
        requested: usize,
        limit: usize,
    },
    Backpressure(String),
    Storage(String),
    DurableDataCorrupt {
        stage: DurableIntegrityStage,
        manifest_index: u64,
        locator: String,
    },
    /// Name-level exhaustive serde/mapping mirror of [`EngineError::ChangeRecordsRequireDurableLog`].
    /// Startup-only; production commit outcomes must never contain this class (TD-008).
    ChangeRecordsRequireDurableLog,
}

impl CommitRejection {
    /// Project an [`EngineError`] to its durable, serializable form.
    pub fn from_error(e: &EngineError) -> Self {
        match e {
            EngineError::NotFound => CommitRejection::NotFound,
            EngineError::QueueDefinitionConflict => CommitRejection::QueueDefinitionConflict,
            EngineError::Invalid(why) => CommitRejection::Invalid((*why).to_string()),
            EngineError::Terminal => CommitRejection::Terminal,
            EngineError::StaleLease => CommitRejection::StaleLease,
            EngineError::Superseded => CommitRejection::Superseded,
            EngineError::Unavailable => CommitRejection::Unavailable,
            EngineError::Conflict => CommitRejection::Conflict,
            EngineError::BatchTooLarge => CommitRejection::BatchTooLarge,
            EngineError::RequestIdConflict => CommitRejection::RequestIdConflict,
            EngineError::Paused { drain_intake } => CommitRejection::Paused {
                drain_intake: *drain_intake,
            },
            EngineError::RequestExpired => CommitRejection::RequestExpired,
            EngineError::EpochFenced => CommitRejection::EpochFenced,
            EngineError::Forbidden(why) => CommitRejection::Forbidden((*why).to_string()),
            EngineError::EntitySchemaViolation(msg) => {
                CommitRejection::EntitySchemaViolation(msg.clone())
            }
            EngineError::RequestTooLarge { requested, limit } => CommitRejection::RequestTooLarge {
                requested: *requested,
                limit: *limit,
            },
            EngineError::Backpressure { resource } => {
                CommitRejection::Backpressure((*resource).to_string())
            }
            EngineError::Storage(msg) => CommitRejection::Storage(msg.clone()),
            EngineError::DurableDataCorrupt {
                stage,
                manifest_index,
                locator,
            } => CommitRejection::DurableDataCorrupt {
                stage: *stage,
                manifest_index: *manifest_index,
                locator: locator.clone(),
            },
            EngineError::ChangeRecordsRequireDurableLog => {
                CommitRejection::ChangeRecordsRequireDurableLog
            }
        }
    }

    /// Reconstruct the [`EngineError`] from its durable form. The two `Invalid` reasons the commit path emits
    /// round-trip to their exact `&'static str`; any other reason (unreachable on the commit path) falls back
    /// to a stable static so the variant is still preserved.
    pub fn into_error(self) -> EngineError {
        match self {
            CommitRejection::NotFound => EngineError::NotFound,
            CommitRejection::QueueDefinitionConflict => EngineError::QueueDefinitionConflict,
            CommitRejection::Invalid(why) => EngineError::Invalid(match why.as_str() {
                "item is not leased" => "item is not leased",
                "instance fence is not monotonic" => "instance fence is not monotonic",
                _ => "invalid",
            }),
            CommitRejection::Terminal => EngineError::Terminal,
            CommitRejection::StaleLease => EngineError::StaleLease,
            CommitRejection::Superseded => EngineError::Superseded,
            CommitRejection::Unavailable => EngineError::Unavailable,
            CommitRejection::Conflict => EngineError::Conflict,
            CommitRejection::BatchTooLarge => EngineError::BatchTooLarge,
            CommitRejection::RequestIdConflict => EngineError::RequestIdConflict,
            CommitRejection::Paused { drain_intake } => EngineError::Paused { drain_intake },
            CommitRejection::RequestExpired => EngineError::RequestExpired,
            CommitRejection::EpochFenced => EngineError::EpochFenced,
            CommitRejection::Forbidden(_) => EngineError::Forbidden("forbidden"),
            CommitRejection::EntitySchemaViolation(msg) => EngineError::EntitySchemaViolation(msg),
            CommitRejection::RequestTooLarge { requested, limit } => {
                EngineError::RequestTooLarge { requested, limit }
            }
            CommitRejection::Backpressure(resource) => EngineError::Backpressure {
                resource: match resource.as_str() {
                    "buffered bytes" => "buffered bytes",
                    "buffered bytes closed" => "buffered bytes closed",
                    "queue buffered bytes" => "queue buffered bytes",
                    _ => "bounded resource",
                },
            },
            CommitRejection::Storage(msg) => EngineError::Storage(msg),
            CommitRejection::DurableDataCorrupt {
                stage,
                manifest_index,
                locator,
            } => EngineError::DurableDataCorrupt {
                stage,
                manifest_index,
                locator,
            },
            CommitRejection::ChangeRecordsRequireDurableLog => {
                EngineError::ChangeRecordsRequireDurableLog
            }
        }
    }
}

pub type EngineResult<T> = Result<T, EngineError>;

#[cfg(test)]
mod commit_rejection_tests {
    use super::*;

    /// Every rejection the commit-validate path can emit must round-trip BYTE-IDENTICALLY through the durable
    /// projection (this is what makes a mixed commit's Rejected entries replay with their exact structured
    /// error across a restart).
    #[test]
    fn commit_rejections_round_trip_identically() {
        let cases = [
            EngineError::NotFound,
            EngineError::StaleLease,
            EngineError::Terminal,
            EngineError::Superseded,
            EngineError::Conflict,
            EngineError::Invalid("item is not leased"),
            EngineError::Invalid("instance fence is not monotonic"),
            EngineError::EntitySchemaViolation("bad doc".into()),
            EngineError::RequestTooLarge {
                requested: 9,
                limit: 8,
            },
            EngineError::Backpressure {
                resource: "buffered bytes",
            },
            EngineError::DurableDataCorrupt {
                stage: DurableIntegrityStage::FrameCrc32c,
                manifest_index: 42,
                locator: "0123456789abcdef".to_owned(),
            },
        ];
        for e in cases {
            let projected = CommitRejection::from_error(&e);
            let json = serde_json::to_string(&projected).unwrap();
            let decoded: CommitRejection = serde_json::from_str(&json).unwrap();
            assert_eq!(
                decoded.into_error(),
                e,
                "commit rejection {e:?} must round-trip byte-identically"
            );
        }
    }

    /// Startup-only `ChangeRecordsRequireDurableLog` exists in the name-level mirror so serde tables
    /// stay exhaustive; mapping fixtures may round-trip it, but production commits never emit it.
    #[test]
    fn change_records_require_durable_log_mirror_round_trips_in_mapping_fixtures() {
        let error = EngineError::ChangeRecordsRequireDurableLog;
        let projected = CommitRejection::from_error(&error);
        assert_eq!(projected, CommitRejection::ChangeRecordsRequireDurableLog);
        let json = serde_json::to_string(&projected).expect("serialize");
        let decoded: CommitRejection = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded.into_error(), error);
    }

    /// Backpressure keeps its existing payload normalization rather than false Rust-shape identity.
    #[test]
    fn backpressure_normalizes_resource_string_in_mirror() {
        let error = EngineError::Backpressure {
            resource: "buffered bytes",
        };
        let projected = CommitRejection::from_error(&error);
        assert_eq!(
            projected,
            CommitRejection::Backpressure("buffered bytes".into())
        );
        assert_eq!(projected.clone().into_error(), error);

        let unknown = CommitRejection::Backpressure("other-resource".into());
        assert_eq!(
            unknown.into_error(),
            EngineError::Backpressure {
                resource: "bounded resource",
            }
        );
    }

    #[test]
    fn resp_token_table_is_exhaustive_for_generic_tokens() {
        let cases: &[(EngineError, Option<&str>)] = &[
            (EngineError::Invalid("x"), Some("-ERR fireweed invalid")),
            (EngineError::Terminal, Some("-ERR fireweed terminal")),
            (EngineError::StaleLease, Some("-ERR fireweed stale_lease")),
            (EngineError::Superseded, Some("-ERR fireweed superseded")),
            (EngineError::Unavailable, Some("-ERR fireweed unavailable")),
            (EngineError::Conflict, Some("-ERR fireweed conflict")),
            (
                EngineError::BatchTooLarge,
                Some("-ERR fireweed batch_too_large"),
            ),
            (
                EngineError::RequestIdConflict,
                Some("-ERR fireweed request_id_conflict"),
            ),
            (
                EngineError::Paused {
                    drain_intake: false,
                },
                Some("-ERR fireweed paused"),
            ),
            (
                EngineError::RequestExpired,
                Some("-ERR fireweed request_expired"),
            ),
            (EngineError::EpochFenced, Some("-ERR fireweed epoch_stale")),
            (
                EngineError::EntitySchemaViolation("x".into()),
                Some("-ERR fireweed entity_schema_violation"),
            ),
            (
                EngineError::RequestTooLarge {
                    requested: 1,
                    limit: 0,
                },
                Some("-ERR fireweed invalid"),
            ),
            (
                EngineError::Backpressure {
                    resource: "buffered bytes",
                },
                Some("-ERR fireweed unavailable"),
            ),
            (
                EngineError::QueueDefinitionConflict,
                Some("-ERR fireweed queue_conflict"),
            ),
            (
                EngineError::ChangeRecordsRequireDurableLog,
                Some("-ERR fireweed change_records_require_durable_log"),
            ),
            (EngineError::NotFound, None),
            (EngineError::Forbidden("x"), None),
            (EngineError::Storage("x".into()), None),
            (
                EngineError::DurableDataCorrupt {
                    stage: DurableIntegrityStage::Manifest,
                    manifest_index: 0,
                    locator: "x".into(),
                },
                None,
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(error.resp_token(), *expected, "{error:?}");
        }
    }
}
