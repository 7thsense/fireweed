//! Engine error model. The RESP adapter maps these to the canonical `-ERR pqueue …` replies
//! (TD-006 §7; asserted verbatim by conformance).

/// Errors a port may return. The variant set is the engine's; adapters translate to their wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineError {
    /// Queue (or shard) not found.
    NotFound,
    /// Queue already exists with an incompatible definition.
    QueueDefinitionConflict,
    /// A lifecycle transition is not allowed on the target (e.g. upsert of a claimed item).
    /// → `-ERR pqueue invalid`.
    Invalid(&'static str),
    /// The target item is terminal. → `-ERR pqueue terminal`.
    Terminal,
    /// The lease has been operator-fenced (stale generation). → `-ERR pqueue stale_lease`.
    StaleLease,
    /// The addressed id was superseded by a pending-item replacement. → `-ERR pqueue superseded`.
    Superseded,
    /// The operation is unavailable on this backend's durability class (e.g. upsert on
    /// eventual-apply). → `-ERR pqueue unavailable`.
    Unavailable,
    /// An optimistic-concurrency or cohort conflict. → `-ERR pqueue conflict`.
    Conflict,
    /// Underlying storage failure (adapter-level).
    Storage(String),
}

impl EngineError {
    /// The canonical `-ERR pqueue …` token a RESP adapter emits for this error, or `None` for
    /// errors that have a non-`-ERR` mapping (e.g. `NotFound` → nil) handled by the adapter.
    pub fn resp_token(&self) -> Option<&'static str> {
        match self {
            EngineError::Invalid(_) => Some("-ERR pqueue invalid"),
            EngineError::Terminal => Some("-ERR pqueue terminal"),
            EngineError::StaleLease => Some("-ERR pqueue stale_lease"),
            EngineError::Superseded => Some("-ERR pqueue superseded"),
            EngineError::Unavailable => Some("-ERR pqueue unavailable"),
            EngineError::Conflict => Some("-ERR pqueue conflict"),
            EngineError::NotFound
            | EngineError::QueueDefinitionConflict
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
            EngineError::Storage(msg) => write!(f, "storage: {msg}"),
        }
    }
}

impl std::error::Error for EngineError {}

pub type EngineResult<T> = Result<T, EngineError>;
