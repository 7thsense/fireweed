//! Fixed-width source ordering facts shared by producers and future readers.
//!
//! Membership is independent of wall time. Readers intersect these complete
//! ranges at one cut; a posting alone never establishes claim eligibility.
use crate::queue_group_state_authority::WorkMetadata;
use fireweed_core::queue_contract::{Lifecycle, QueueError, QueueResult};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum QueueSourceWorkIndex {
    RawLifecycle = 0,
    Attempts = 1,
    LeaseExpiry = 2,
    PriorityAscending = 3,
    PriorityDescending = 4,
    /// All live work, including terminal work: also the administration
    /// AvailableAt index. Claim readers must intersect effective Pending.
    Schedule = 5,
    OverduePriorityAscending = 6,
    OverduePriorityDescending = 7,
    OverdueSchedule = 8,
}
impl QueueSourceWorkIndex {
    pub const ALL: [Self; 9] = [
        Self::RawLifecycle,
        Self::Attempts,
        Self::LeaseExpiry,
        Self::PriorityAscending,
        Self::PriorityDescending,
        Self::Schedule,
        Self::OverduePriorityAscending,
        Self::OverduePriorityDescending,
        Self::OverdueSchedule,
    ];
    pub(super) fn tag(self) -> u8 {
        self as u8
    }
    pub(super) fn width(self) -> usize {
        match self {
            Self::RawLifecycle => 1,
            Self::Attempts => 4,
            Self::LeaseExpiry => 8,
            Self::PriorityAscending | Self::PriorityDescending | Self::Schedule => 16,
            Self::OverduePriorityAscending
            | Self::OverduePriorityDescending
            | Self::OverdueSchedule => 24,
        }
    }
    /// Family prefix, without a numeric component or literal-key terminator.
    pub fn prefix(self) -> [u8; 3] {
        [super::key::VERSION, 11, self.tag()]
    }
    /// A complete first numeric component, including neither later numeric
    /// components nor a key terminator. Its successor excludes that entire tie
    /// group. Descending priority reverses numeric before/after, not sequence.
    pub fn value_prefix(self, value: QueueSourceWorkValue) -> QueueResult<QueueSourceWorkPrefix> {
        let mut out = QueueSourceWorkPrefix {
            bytes: [0; 11],
            length: 3,
        };
        out.bytes[..3].copy_from_slice(&self.prefix());
        match (self, value) {
            (Self::RawLifecycle, QueueSourceWorkValue::Lifecycle(lifecycle)) => {
                out.bytes[3] = lifecycle_tag(lifecycle);
                out.length = 4;
            }
            (Self::Attempts, QueueSourceWorkValue::Attempts(attempts)) => {
                out.bytes[3..7].copy_from_slice(&attempts.to_be_bytes());
                out.length = 7;
            }
            (Self::RawLifecycle | Self::Attempts, _)
            | (_, QueueSourceWorkValue::Lifecycle(_) | QueueSourceWorkValue::Attempts(_)) => {
                return Err(unknown("numeric prefix type differs from index"));
            }
            (_, QueueSourceWorkValue::Signed(value)) => {
                let mut bytes = signed(value);
                if self == Self::PriorityDescending {
                    bytes.iter_mut().for_each(|byte| *byte = !*byte);
                }
                out.bytes[3..11].copy_from_slice(&bytes);
                out.length = 11;
            }
        }
        Ok(out)
    }
    /// Produce the canonical static membership and fixed-width ordering tuple.
    /// Version, payload, lease token and unrelated fields are deliberately absent.
    pub fn posting(self, metadata: WorkMetadata) -> QueueResult<Option<QueueSourceWorkPosting>> {
        if metadata.version == 0
            || metadata.insertion_sequence == 0
            || (metadata.lifecycle == Lifecycle::Leased) != metadata.lease_expires_at.is_some()
            || metadata.lease_expires_at.is_some() != metadata.lease_hash.is_some()
        {
            return Err(unknown("invalid work metadata"));
        }
        let candidate = matches!(metadata.lifecycle, Lifecycle::Pending | Lifecycle::Leased);
        let mut out = QueueSourceWorkPosting {
            index: self,
            bytes: [0; 24],
        };
        match self {
            Self::RawLifecycle => out.bytes[0] = lifecycle_tag(metadata.lifecycle),
            Self::Attempts => out.bytes[..4].copy_from_slice(&metadata.attempts.to_be_bytes()),
            Self::LeaseExpiry => {
                let Some(expiry) = metadata.lease_expires_at else {
                    return Ok(None);
                };
                out.bytes[..8].copy_from_slice(&signed(expiry));
            }
            _ => {
                if self != Self::Schedule && !candidate {
                    return Ok(None);
                }
                let overdue = matches!(
                    self,
                    Self::OverduePriorityAscending
                        | Self::OverduePriorityDescending
                        | Self::OverdueSchedule
                );
                let offset = if overdue {
                    out.bytes[..8].copy_from_slice(&signed(metadata.eligible_since));
                    8
                } else {
                    0
                };
                let primary = if matches!(self, Self::Schedule | Self::OverdueSchedule) {
                    metadata.available_at
                } else {
                    metadata.priority
                };
                let mut primary = signed(primary);
                if matches!(
                    self,
                    Self::PriorityDescending | Self::OverduePriorityDescending
                ) {
                    primary.iter_mut().for_each(|byte| *byte = !*byte);
                }
                out.bytes[offset..offset + 8].copy_from_slice(&primary);
                out.bytes[offset + 8..offset + 16]
                    .copy_from_slice(&metadata.insertion_sequence.to_be_bytes());
            }
        }
        Ok(Some(out))
    }
}
#[derive(Clone, Copy)]
pub enum QueueSourceWorkValue {
    Lifecycle(Lifecycle),
    Attempts(u32),
    /// The first signed component: expiry, priority, schedule or overdue age.
    Signed(i64),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueSourceWorkPosting {
    index: QueueSourceWorkIndex,
    bytes: [u8; 24],
}
impl QueueSourceWorkPosting {
    pub fn index(self) -> QueueSourceWorkIndex {
        self.index
    }
    pub fn sort(&self) -> &[u8] {
        &self.bytes[..self.index.width()]
    }
}
#[derive(Clone, Copy)]
pub struct QueueSourceWorkPrefix {
    bytes: [u8; 11],
    length: usize,
}
impl QueueSourceWorkPrefix {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.length]
    }
}
fn signed(value: i64) -> [u8; 8] {
    ((value as u64) ^ (1 << 63)).to_be_bytes()
}
fn lifecycle_tag(lifecycle: Lifecycle) -> u8 {
    match lifecycle {
        Lifecycle::Pending => 0,
        Lifecycle::Leased => 1,
        Lifecycle::Completed => 2,
        Lifecycle::Failed => 3,
    }
}
fn unknown(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source intrinsic index: {message}"))
}

#[cfg(test)]
mod tests;
