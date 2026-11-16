//! Pure checked runtime defaults shared by concrete API-007 constructors.
//! This is resource configuration, not a durable queue definition or DTO.
use fireweed_core::{
    queue_contract::{QueueError, QueueLimits, QueueResult},
    queue_runtime::QueueRuntimeOptions,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedQueueRuntime {
    operations: usize,
    retained_bytes: u64,
    scratch_bytes: u64,
    scratch_workers: usize,
    scratch_root: Option<std::path::PathBuf>,
}
impl ValidatedQueueRuntime {
    /// Derive existing defaults without creating threads, directories or clients.
    pub fn for_limits(limits: &QueueLimits) -> QueueResult<Self> {
        Self::with_options(limits, QueueRuntimeOptions::default(), 2)
    }
    /// Resolve and validate all resource settings without I/O or thread creation.
    pub fn with_options(
        limits: &QueueLimits,
        options: QueueRuntimeOptions,
        default_workers: usize,
    ) -> QueueResult<Self> {
        limits.validate()?;
        if options
            .scratch_root
            .as_ref()
            .is_some_and(|path| !path.is_absolute())
        {
            return Err(QueueError::Invalid("scratch root must be absolute".into()));
        }
        let scratch_workers = options.scratch_workers.unwrap_or(default_workers);
        // The portable bound excludes semaphore overflow and impossible owned
        // worker configurations before any runtime or backing-service effects.
        if options.operations == 0
            || options.operations > (usize::MAX >> 3)
            || scratch_workers == 0
            || scratch_workers > options.operations
        {
            return Err(QueueError::Invalid(
                "invalid queue execution capacities".into(),
            ));
        }
        let retained_bytes = match options.retained_bytes {
            Some(bytes) => bytes,
            None => limits
                .max_request_bytes
                .checked_mul(8)
                .ok_or(QueueError::TooLarge)?,
        };
        let scratch_bytes = match options.scratch_bytes {
            Some(bytes) => bytes,
            None => limits
                .max_group_bytes
                .checked_mul(16)
                .ok_or(QueueError::TooLarge)?,
        };
        if retained_bytes == 0 || scratch_bytes == 0 {
            return Err(QueueError::Invalid(
                "queue byte capacities must be positive".into(),
            ));
        }
        Ok(Self {
            operations: options.operations,
            retained_bytes,
            scratch_bytes,
            scratch_workers,
            scratch_root: options.scratch_root,
        })
    }
    pub fn operations(&self) -> usize {
        self.operations
    }
    pub fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }
    pub fn scratch_workers(&self) -> usize {
        self.scratch_workers
    }
    pub fn scratch_bytes(&self) -> u64 {
        self.scratch_bytes
    }
    pub fn resolve_scratch_root(
        self,
        default: impl FnOnce() -> std::path::PathBuf,
    ) -> QueueResult<std::path::PathBuf> {
        let path = self.scratch_root.unwrap_or_else(default);
        if !path.is_absolute() {
            return Err(QueueError::Invalid("scratch root must be absolute".into()));
        }
        Ok(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scratch_default_is_resolved_once_and_validated_without_environment_mutation() {
        let limits = QueueLimits::default();
        let calls = std::cell::Cell::new(0);
        let runtime = ValidatedQueueRuntime::for_limits(&limits).unwrap();
        let resolved = runtime
            .resolve_scratch_root(|| {
                calls.set(calls.get() + 1);
                std::path::PathBuf::from("/first-root")
            })
            .unwrap();
        assert_eq!(calls.get(), 1);
        assert_eq!(resolved, std::path::PathBuf::from("/first-root"));
        assert_eq!(
            ValidatedQueueRuntime::for_limits(&limits)
                .unwrap()
                .resolve_scratch_root(|| std::path::PathBuf::from("relative")),
            Err(QueueError::Invalid("scratch root must be absolute".into()))
        );
        let runtime = ValidatedQueueRuntime::with_options(
            &limits,
            QueueRuntimeOptions {
                scratch_root: Some(std::path::PathBuf::from("/explicit-root")),
                ..Default::default()
            },
            2,
        )
        .unwrap();
        assert_eq!(
            runtime
                .resolve_scratch_root(|| panic!("explicit root must bypass default"))
                .unwrap(),
            std::path::PathBuf::from("/explicit-root")
        );
    }
    #[test]
    fn explicit_capacity_does_not_evaluate_unused_default_multipliers() {
        let limits = QueueLimits {
            max_request_bytes: u64::MAX,
            max_group_bytes: u64::MAX,
            ..Default::default()
        };
        let runtime = ValidatedQueueRuntime::with_options(
            &limits,
            QueueRuntimeOptions {
                operations: 3,
                retained_bytes: Some(17),
                scratch_bytes: Some(29),
                scratch_workers: Some(1),
                scratch_root: None,
            },
            32,
        )
        .unwrap();
        assert_eq!(runtime.operations(), 3);
        assert_eq!(runtime.retained_bytes(), 17);
        assert_eq!(runtime.scratch_bytes(), 29);
        assert_eq!(runtime.scratch_workers(), 1);
        assert_eq!(
            ValidatedQueueRuntime::for_limits(&limits),
            Err(QueueError::TooLarge)
        );
    }
}
