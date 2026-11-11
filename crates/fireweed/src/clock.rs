use fireweed_core::UtcTimestamp;
use fireweed_engine::Clock;

/// Wall-clock [`Clock`] for production use, e.g. `Arc::new(SystemClock)` for [`crate::queue::Queue`]
/// constructors that take a clock. Provided here so a consumer depending on `fireweed` alone has a
/// ready clock without naming `fireweed-engine`.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> UtcTimestamp {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        UtcTimestamp::new(d.as_secs() as i64, d.subsec_nanos()).expect("valid unix ts")
    }
}
