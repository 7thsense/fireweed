//! Typed process-local capacities. These are not durable queue policy, request
//! identity, or a transport-specific capability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueRuntimeOptions {
    /// Shared maximum outstanding operations across every handle clone.
    pub operations: usize,
    /// None derives eight times the common request limit, with checked arithmetic.
    pub retained_bytes: Option<u64>,
    /// None derives sixteen times the common group limit, with checked arithmetic.
    pub scratch_bytes: Option<u64>,
    /// None uses the constructor's documented worker default.
    pub scratch_workers: Option<usize>,
    /// Explicit process-local temporary root. PostgreSQL resolves None to the
    /// OS temporary directory once at construction; operations never reread it.
    pub scratch_root: Option<std::path::PathBuf>,
}
impl Default for QueueRuntimeOptions {
    fn default() -> Self {
        Self {
            operations: 64,
            retained_bytes: None,
            scratch_bytes: None,
            scratch_workers: None,
            scratch_root: None,
        }
    }
}
