//! Backend-neutral pure policy for object-log maintenance.

use std::collections::BTreeSet;

use crate::QueueKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MaintenanceObjectClass {
    SegmentPrefix,
    ManifestEntry,
    OrphanManifestCandidate,
    OrphanSegmentAttempt,
    OrphanBranch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MaintenanceReason {
    Eligible,
    Filtered,
    ProjectionUnhealthy,
    CompleteFrontierMissing,
    SnapshotNotCovered,
    RecoveryWindowActive,
    ManifestTailRequired,
    RequestIdRequired,
    ItemKeyRequired,
    AsyncProjectionRequired,
    InMemoryClaimReplayPinned,
    BranchPinned,
    InFlightWriterGrace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceDisposition {
    Delete,
    Retain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrontierRequirement {
    Unknown,
    NotRequired,
    RequiredFrom(u64),
}

/// One immutable authority snapshot. `None` means evidence is unavailable, not permission to ignore an axis.
/// Non-async profiles set `complete_frontier_required=false` and use the proven legacy checkpoint/window pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceAuthoritySnapshot {
    pub queue: QueueKey,
    pub current_epoch: u64,
    pub observed_at_ms: i64,
    pub retention_may_advance: bool,
    pub complete_frontier_required: bool,
    pub lineage_validated: bool,
    pub committed_snapshot_through: Option<u64>,
    pub recovery_window_through: Option<u64>,
    pub manifest_tail: FrontierRequirement,
    pub request_ids: FrontierRequirement,
    pub item_keys: FrontierRequirement,
    pub async_projection_through: Option<u64>,
    pub in_memory_claim_replay: FrontierRequirement,
    pub durable_floor: Option<u64>,
    pub branch_pins: BTreeSet<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceCandidate {
    pub queue: QueueKey,
    pub stable_id: String,
    pub class: MaintenanceObjectClass,
    pub first_sequence: Option<u64>,
    pub last_sequence: Option<u64>,
    pub manifest_index: Option<u64>,
    pub bytes: Option<u64>,
    pub created_at_ms: i64,
    /// Authoritative reference resolution proved this object is not named by a winning head/marker.
    pub unreferenced_proven: bool,
    /// The exact successor/head decision proved this attempt lost, rather than merely being absent from LIST.
    pub loser_proven: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MaintenanceFilter {
    pub object_classes: BTreeSet<MaintenanceObjectClass>,
    pub queue: Option<QueueKey>,
    pub min_age_ms: Option<u64>,
    pub reasons: BTreeSet<MaintenanceReason>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceDecision {
    pub candidate: MaintenanceCandidate,
    pub disposition: MaintenanceDisposition,
    pub reason: MaintenanceReason,
    pub snapshot_epoch: u64,
    pub frontier: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenancePolicy {
    pub orphan_grace_ms: u64,
}

impl MaintenancePolicy {
    pub fn new(orphan_grace_ms: u64) -> Self {
        Self { orphan_grace_ms }
    }

    pub fn plan(
        &self,
        authority: &MaintenanceAuthoritySnapshot,
        candidates: &[MaintenanceCandidate],
        filter: &MaintenanceFilter,
    ) -> Vec<MaintenanceDecision> {
        let frontier = self.frontier(authority);
        let mut ordered = candidates.to_vec();
        ordered.sort_by(|a, b| {
            a.class
                .cmp(&b.class)
                .then(a.first_sequence.cmp(&b.first_sequence))
                .then(a.manifest_index.cmp(&b.manifest_index))
                .then(a.stable_id.cmp(&b.stable_id))
        });
        ordered
            .into_iter()
            .map(|candidate| {
                let mut reason = self.reason(authority, &candidate, filter, frontier);
                if reason == MaintenanceReason::Eligible
                    && !filter.reasons.is_empty()
                    && !filter.reasons.contains(&MaintenanceReason::Eligible)
                {
                    reason = MaintenanceReason::Filtered;
                }
                let disposition = if reason == MaintenanceReason::Eligible
                    && (filter.reasons.is_empty()
                        || filter.reasons.contains(&MaintenanceReason::Eligible))
                {
                    MaintenanceDisposition::Delete
                } else {
                    MaintenanceDisposition::Retain
                };
                MaintenanceDecision {
                    candidate,
                    disposition,
                    reason,
                    snapshot_epoch: authority.current_epoch,
                    frontier,
                }
            })
            .collect()
    }

    fn frontier(&self, authority: &MaintenanceAuthoritySnapshot) -> Option<u64> {
        if !authority.retention_may_advance || !authority.lineage_validated {
            return None;
        }
        let mut frontier = authority
            .committed_snapshot_through?
            .min(authority.recovery_window_through?);
        if authority.complete_frontier_required {
            frontier = frontier.min(authority.async_projection_through?);
            for requirement in [
                authority.manifest_tail,
                authority.request_ids,
                authority.item_keys,
                authority.in_memory_claim_replay,
            ] {
                match requirement {
                    FrontierRequirement::Unknown => return None,
                    FrontierRequirement::NotRequired => {}
                    FrontierRequirement::RequiredFrom(required) => {
                        if required == 0 {
                            return None;
                        }
                        frontier = frontier.min(required - 1);
                    }
                }
            }
        } else if let FrontierRequirement::RequiredFrom(required) = authority.in_memory_claim_replay
        {
            if required == 0 {
                return None;
            }
            frontier = frontier.min(required - 1);
        }
        Some(frontier)
    }

    fn reason(
        &self,
        authority: &MaintenanceAuthoritySnapshot,
        candidate: &MaintenanceCandidate,
        filter: &MaintenanceFilter,
        frontier: Option<u64>,
    ) -> MaintenanceReason {
        if candidate.queue != authority.queue
            || filter
                .queue
                .as_ref()
                .is_some_and(|queue| queue != &candidate.queue)
            || (!filter.object_classes.is_empty()
                && !filter.object_classes.contains(&candidate.class))
            || filter.min_age_ms.is_some_and(|age| {
                u64::try_from(
                    authority
                        .observed_at_ms
                        .saturating_sub(candidate.created_at_ms)
                        .max(0),
                )
                .unwrap_or(u64::MAX)
                    < age
            })
        {
            return MaintenanceReason::Filtered;
        }
        if !authority.retention_may_advance {
            return MaintenanceReason::ProjectionUnhealthy;
        }
        if !authority.lineage_validated {
            return MaintenanceReason::CompleteFrontierMissing;
        }
        if matches!(
            candidate.class,
            MaintenanceObjectClass::OrphanBranch
                | MaintenanceObjectClass::OrphanManifestCandidate
                | MaintenanceObjectClass::OrphanSegmentAttempt
        ) {
            if !candidate.unreferenced_proven {
                return MaintenanceReason::ManifestTailRequired;
            }
            if matches!(
                candidate.class,
                MaintenanceObjectClass::OrphanManifestCandidate
                    | MaintenanceObjectClass::OrphanSegmentAttempt
            ) && !candidate.loser_proven
            {
                return MaintenanceReason::ManifestTailRequired;
            }
            return if u64::try_from(
                authority
                    .observed_at_ms
                    .saturating_sub(candidate.created_at_ms)
                    .max(0),
            )
            .unwrap_or(u64::MAX)
                < self.orphan_grace_ms
            {
                MaintenanceReason::InFlightWriterGrace
            } else {
                MaintenanceReason::Eligible
            };
        }
        let Some(last) = candidate.last_sequence else {
            return MaintenanceReason::ManifestTailRequired;
        };
        if candidate
            .first_sequence
            .is_some_and(|first| authority.branch_pins.iter().any(|cut| first <= *cut))
        {
            return MaintenanceReason::BranchPinned;
        }
        let Some(frontier) = frontier else {
            return if authority.committed_snapshot_through.is_none() {
                MaintenanceReason::SnapshotNotCovered
            } else if authority.recovery_window_through.is_none() {
                MaintenanceReason::RecoveryWindowActive
            } else if authority.complete_frontier_required
                && authority.manifest_tail == FrontierRequirement::Unknown
            {
                MaintenanceReason::ManifestTailRequired
            } else if authority.complete_frontier_required
                && authority.request_ids == FrontierRequirement::Unknown
            {
                MaintenanceReason::RequestIdRequired
            } else if authority.complete_frontier_required
                && authority.item_keys == FrontierRequirement::Unknown
            {
                MaintenanceReason::ItemKeyRequired
            } else if authority.complete_frontier_required
                && authority.async_projection_through.is_none()
            {
                MaintenanceReason::AsyncProjectionRequired
            } else {
                MaintenanceReason::CompleteFrontierMissing
            };
        };
        if last > frontier {
            return MaintenanceReason::RecoveryWindowActive;
        }
        MaintenanceReason::Eligible
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_core::{QueueId, TenantId};

    fn queue() -> QueueKey {
        QueueKey::new(TenantId::new("t").unwrap(), QueueId::new("q").unwrap())
    }

    fn authority() -> MaintenanceAuthoritySnapshot {
        MaintenanceAuthoritySnapshot {
            queue: queue(),
            current_epoch: 2,
            observed_at_ms: 1000,
            retention_may_advance: true,
            complete_frontier_required: false,
            lineage_validated: true,
            committed_snapshot_through: Some(100),
            recovery_window_through: Some(80),
            manifest_tail: FrontierRequirement::NotRequired,
            request_ids: FrontierRequirement::NotRequired,
            item_keys: FrontierRequirement::NotRequired,
            async_projection_through: None,
            in_memory_claim_replay: FrontierRequirement::NotRequired,
            durable_floor: Some(10),
            branch_pins: BTreeSet::new(),
        }
    }

    #[test]
    fn deterministic_policy_covers_filters_and_complete_frontier() {
        let candidate = MaintenanceCandidate {
            queue: queue(),
            stable_id: "s".into(),
            class: MaintenanceObjectClass::SegmentPrefix,
            first_sequence: Some(20),
            last_sequence: Some(70),
            manifest_index: None,
            bytes: Some(10),
            created_at_ms: 0,
            unreferenced_proven: true,
            loser_proven: false,
        };
        let decision = MaintenancePolicy::new(10).plan(
            &authority(),
            std::slice::from_ref(&candidate),
            &MaintenanceFilter::default(),
        );
        assert_eq!(decision[0].disposition, MaintenanceDisposition::Delete);
        let mut async_authority = authority();
        async_authority.complete_frontier_required = true;
        assert_eq!(
            MaintenancePolicy::new(10).plan(
                &async_authority,
                &[candidate],
                &MaintenanceFilter::default()
            )[0]
            .reason,
            MaintenanceReason::AsyncProjectionRequired
        );
    }

    #[test]
    fn complete_frontier_reports_each_missing_authority_axis() {
        let candidate = MaintenanceCandidate {
            queue: queue(),
            stable_id: "s".into(),
            class: MaintenanceObjectClass::SegmentPrefix,
            first_sequence: Some(20),
            last_sequence: Some(70),
            manifest_index: None,
            bytes: None,
            created_at_ms: 0,
            unreferenced_proven: true,
            loser_proven: false,
        };
        let policy = MaintenancePolicy::new(0);
        let mut base = authority();
        base.complete_frontier_required = true;
        base.async_projection_through = Some(90);
        base.manifest_tail = FrontierRequirement::NotRequired;
        base.request_ids = FrontierRequirement::NotRequired;
        base.item_keys = FrontierRequirement::NotRequired;
        base.in_memory_claim_replay = FrontierRequirement::NotRequired;
        type MissingAuthorityCase = (fn(&mut MaintenanceAuthoritySnapshot), MaintenanceReason);
        let cases: [MissingAuthorityCase; 6] = [
            (
                |a: &mut MaintenanceAuthoritySnapshot| a.committed_snapshot_through = None,
                MaintenanceReason::SnapshotNotCovered,
            ),
            (
                |a: &mut MaintenanceAuthoritySnapshot| a.recovery_window_through = None,
                MaintenanceReason::RecoveryWindowActive,
            ),
            (
                |a: &mut MaintenanceAuthoritySnapshot| {
                    a.manifest_tail = FrontierRequirement::Unknown
                },
                MaintenanceReason::ManifestTailRequired,
            ),
            (
                |a: &mut MaintenanceAuthoritySnapshot| a.request_ids = FrontierRequirement::Unknown,
                MaintenanceReason::RequestIdRequired,
            ),
            (
                |a: &mut MaintenanceAuthoritySnapshot| a.item_keys = FrontierRequirement::Unknown,
                MaintenanceReason::ItemKeyRequired,
            ),
            (
                |a: &mut MaintenanceAuthoritySnapshot| a.async_projection_through = None,
                MaintenanceReason::AsyncProjectionRequired,
            ),
        ];
        for (mutate, expected) in cases {
            let mut snapshot = base.clone();
            mutate(&mut snapshot);
            assert_eq!(
                policy.plan(
                    &snapshot,
                    std::slice::from_ref(&candidate),
                    &MaintenanceFilter::default()
                )[0]
                .reason,
                expected
            );
        }
    }

    #[test]
    fn filters_and_orphan_proof_fail_closed() {
        let orphan = MaintenanceCandidate {
            queue: queue(),
            stable_id: "o".into(),
            class: MaintenanceObjectClass::OrphanBranch,
            first_sequence: None,
            last_sequence: None,
            manifest_index: None,
            bytes: None,
            created_at_ms: 0,
            unreferenced_proven: false,
            loser_proven: false,
        };
        let policy = MaintenancePolicy::new(0);
        assert_eq!(
            policy.plan(
                &authority(),
                std::slice::from_ref(&orphan),
                &MaintenanceFilter::default()
            )[0]
            .reason,
            MaintenanceReason::ManifestTailRequired
        );
        let mut proven = orphan;
        proven.unreferenced_proven = true;
        let mut filter = MaintenanceFilter::default();
        filter.reasons.insert(MaintenanceReason::BranchPinned);
        assert_eq!(
            policy.plan(&authority(), &[proven], &filter)[0].reason,
            MaintenanceReason::Filtered
        );
    }

    #[test]
    fn zero_required_frontier_and_cut_range_pin_delete_nothing() {
        let policy = MaintenancePolicy::new(0);
        let mut snapshot = authority();
        snapshot.in_memory_claim_replay = FrontierRequirement::RequiredFrom(0);
        let candidate = MaintenanceCandidate {
            queue: queue(),
            stable_id: "segment-5".into(),
            class: MaintenanceObjectClass::SegmentPrefix,
            first_sequence: Some(5),
            last_sequence: Some(5),
            manifest_index: None,
            bytes: None,
            created_at_ms: 0,
            unreferenced_proven: false,
            loser_proven: false,
        };
        assert_eq!(
            policy.plan(
                &snapshot,
                std::slice::from_ref(&candidate),
                &MaintenanceFilter::default()
            )[0]
            .disposition,
            MaintenanceDisposition::Retain
        );

        snapshot.in_memory_claim_replay = FrontierRequirement::NotRequired;
        snapshot.branch_pins.insert(10);
        assert_eq!(
            policy.plan(&snapshot, &[candidate], &MaintenanceFilter::default())[0].reason,
            MaintenanceReason::BranchPinned
        );
    }
}
