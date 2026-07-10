//! Engine error model. The RESP adapter maps these to the canonical `-ERR pqueue ...` replies
//! (TD-006 section 7; asserted verbatim by conformance).

/// Errors a port may return. The variant set is the engine's; adapters translate to their wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineError {
    /// Queue (or shard) not found.
    NotFound,
    /// Queue already exists with an incompatible definition.
    QueueDefinitionConflict,
    /// A lifecycle transition is not allowed on the target (e.g. upsert of a claimed item).
    /// Maps to `-ERR pqueue invalid`.
    Invalid(&'static str),
    /// The target item is terminal. Maps to `-ERR pqueue terminal`.
    Terminal,
    /// The lease has been operator-fenced (stale generation). Maps to `-ERR pqueue stale_lease`.
    StaleLease,
    /// The addressed id was superseded by a pending-item replacement. Maps to `-ERR pqueue superseded`.
    Superseded,
    /// The operation is unavailable on this backend's durability class (e.g. upsert on
    /// eventual-apply). Maps to `-ERR pqueue unavailable`.
    Unavailable,
    /// An optimistic-concurrency or cohort conflict. Maps to `-ERR pqueue conflict`.
    Conflict,
    /// A claim/cohort/group unit would exceed `max_items` (API-001 `batch-too-large`).
    /// Maps to `-ERR pqueue batch_too_large`.
    BatchTooLarge,
    /// A retried `request_id` carried a different body (API-001 `request-id-conflict`).
    /// Distinct from the generic `Conflict`. Maps to `-ERR pqueue request_id_conflict`.
    RequestIdConflict,
    /// Intake is blocked by a paused queue. The `drain_intake` flag distinguishes the fully-quiesced
    /// branch-prep mode from the legacy "claims stop, pushes still land" pause.
    Paused { drain_intake: bool },
    /// A `request_id` replay arrived after its retention window (API-001 `request-expired`).
    /// Maps to `-ERR pqueue request_expired`.
    RequestExpired,
    /// An append/claim carried an `expected_epoch` that is not the queue's current durable
    /// `assignment_epoch` — a stale (superseded) owner was fenced (TD-003 Single Authoritative Fencing
    /// Rule, `queue-epoch-stale`). Maps to `-ERR pqueue epoch_stale`.
    EpochFenced,
    /// The principal is not authorized (cross-tenant or missing operator privilege). The RESP
    /// adapter maps this to `-NOPERM` (TD-006 section 2); not an `-ERR pqueue ...` reply.
    Forbidden(&'static str),
    /// The item's entity document violates the queue's compiled entity schema (ADR-011). Rejection
    /// happens before log append, idempotency recording, SQL mutation, or projection apply.
    /// Maps to `-ERR pqueue entity_schema_violation`.
    EntitySchemaViolation(String),
    /// Underlying storage failure (adapter-level).
    Storage(String),
}

impl EngineError {
    /// The canonical `-ERR pqueue ...` token a RESP adapter emits for this error, or `None` for
    /// errors that have a non-`-ERR` mapping (e.g. `NotFound` to nil) handled by the adapter.
    pub fn resp_token(&self) -> Option<&'static str> {
        match self {
            EngineError::Invalid(_) => Some("-ERR pqueue invalid"),
            EngineError::Terminal => Some("-ERR pqueue terminal"),
            EngineError::StaleLease => Some("-ERR pqueue stale_lease"),
            EngineError::Superseded => Some("-ERR pqueue superseded"),
            EngineError::Unavailable => Some("-ERR pqueue unavailable"),
            EngineError::Conflict => Some("-ERR pqueue conflict"),
            EngineError::BatchTooLarge => Some("-ERR pqueue batch_too_large"),
            EngineError::RequestIdConflict => Some("-ERR pqueue request_id_conflict"),
            EngineError::Paused { .. } => Some("-ERR pqueue paused"),
            EngineError::RequestExpired => Some("-ERR pqueue request_expired"),
            EngineError::EpochFenced => Some("-ERR pqueue epoch_stale"),
            EngineError::EntitySchemaViolation(_) => Some("-ERR pqueue entity_schema_violation"),
            // Forbidden -> `-NOPERM`, NotFound -> nil: non-`-ERR pqueue` mappings handled by the adapter.
            EngineError::NotFound
            | EngineError::QueueDefinitionConflict
            | EngineError::Forbidden(_)
            | EngineError::Storage(_) => None,
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
            EngineError::Forbidden(why) => write!(f, "forbidden: {why}"),
            EngineError::Storage(msg) => write!(f, "storage: {msg}"),
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
    Paused { drain_intake: bool },
    RequestExpired,
    EpochFenced,
    Forbidden(String),
    EntitySchemaViolation(String),
    Storage(String),
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
            EngineError::Storage(msg) => CommitRejection::Storage(msg.clone()),
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
            CommitRejection::Storage(msg) => EngineError::Storage(msg),
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
}
