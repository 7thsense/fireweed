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
    /// A `request_id` replay arrived after its retention window (API-001 `request-expired`).
    /// Maps to `-ERR pqueue request_expired`.
    RequestExpired,
    /// The principal is not authorized (cross-tenant or missing operator privilege). The RESP
    /// adapter maps this to `-NOPERM` (TD-006 section 2); not an `-ERR pqueue ...` reply.
    Forbidden(&'static str),
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
            EngineError::RequestExpired => Some("-ERR pqueue request_expired"),
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
            EngineError::RequestExpired => write!(f, "request expired"),
            EngineError::Forbidden(why) => write!(f, "forbidden: {why}"),
            EngineError::Storage(msg) => write!(f, "storage: {msg}"),
        }
    }
}

impl std::error::Error for EngineError {}

pub type EngineResult<T> = Result<T, EngineError>;
