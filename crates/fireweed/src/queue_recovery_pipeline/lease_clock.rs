//! The clock an owner measures its lease deadline on. It must count all elapsed
//! time, including process stalls and system suspend.
use std::time::Duration;

pub trait LeaseClock: Send + Sync {
    /// Elapsed time since an arbitrary fixed origin of this clock.
    fn now(&self) -> Duration;
}

/// Linux counts suspend through `CLOCK_BOOTTIME`. Other platforms fall back to
/// `Instant`, which may not advance while the system sleeps; they are for
/// development only.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemLeaseClock;

impl LeaseClock for SystemLeaseClock {
    #[cfg(target_os = "linux")]
    fn now(&self) -> Duration {
        let time = rustix::time::clock_gettime(rustix::time::ClockId::Boottime);
        Duration::new(time.tv_sec as u64, time.tv_nsec as u32)
    }
    #[cfg(not(target_os = "linux"))]
    fn now(&self) -> Duration {
        static ORIGIN: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
        ORIGIN.get_or_init(std::time::Instant::now).elapsed()
    }
}
