//! Per-full-tuple uniqueness reduction for an externally grouped evidence stream.
//! No I/O, sorting, spill implementation or publication authority lives here.
use crate::queue_admission::{QueueBufferCharge, QueueReservation};
use crate::queue_planner::{RecordIdentity, UniqueIdentity};
use fireweed_core::queue_contract::{QueueError, QueueResult};

fn unknown(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(message.into())
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum UniqueClaimOrigin {
    Administrative,
    Ordinary,
}
pub enum UniqueEvidenceEffect<'a> {
    Observe,
    Release(&'a RecordIdentity),
    Claim {
        owner: &'a RecordIdentity,
        origin: UniqueClaimOrigin,
    },
}
/// Ephemeral typed spool record; the full tuple is the physical grouping key.
/// This is not a durable publication encoding or a public consumer DTO.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ScratchUniqueEffect {
    Release(RecordIdentity),
    Claim {
        owner: RecordIdentity,
        origin: UniqueClaimOrigin,
    },
    /// Authority-only observation from a resolved but rejected row.
    Observe,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScratchUniqueEvidence {
    pub initial_loaded: bool,
    pub initial_owner: Option<RecordIdentity>,
    pub effect: ScratchUniqueEffect,
}
impl ScratchUniqueEvidence {
    pub fn borrowed<'a>(&'a self, tuple: &'a UniqueIdentity) -> UniqueEvidence<'a> {
        UniqueEvidence {
            tuple,
            initial_loaded: self.initial_loaded,
            initial_owner: self.initial_owner.as_ref(),
            effect: match &self.effect {
                ScratchUniqueEffect::Observe => UniqueEvidenceEffect::Observe,
                ScratchUniqueEffect::Release(owner) => UniqueEvidenceEffect::Release(owner),
                ScratchUniqueEffect::Claim { owner, origin } => UniqueEvidenceEffect::Claim {
                    owner,
                    origin: *origin,
                },
            },
        }
    }
}
/// Evidence comes from initial fenced authority. Releases/claims require accepted
/// effects; Observe preserves authority consulted by rejected candidates too.
/// Missing probe authority is distinct from a loaded nonownership result.
pub struct UniqueEvidence<'a> {
    pub tuple: &'a UniqueIdentity,
    pub initial_loaded: bool,
    pub initial_owner: Option<&'a RecordIdentity>,
    pub effect: UniqueEvidenceEffect<'a>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UniqueTupleDecision {
    /// Existing administrative sibling-claim conflict precedes ordinary CAS.
    pub administrative_collision: bool,
    /// Combined conflict is considered only after ordinary business validation.
    pub final_collision: bool,
}
struct RetainedOwner<'r> {
    identity: RecordIdentity,
    _charge: QueueBufferCharge<'r>,
}
impl<'r> RetainedOwner<'r> {
    fn copy(owner: &RecordIdentity, reservation: &'r QueueReservation) -> QueueResult<Self> {
        let bytes = (owner.key.len() as u64)
            .checked_add(
                owner
                    .collection
                    .as_ref()
                    .map_or(0, |name| name.len() as u64),
            )
            .and_then(|bytes| bytes.checked_mul(4))
            .and_then(|bytes| bytes.checked_add(1024))
            .ok_or_else(|| unknown("unique owner allocation overflow"))?;
        let charge = reservation.buffer(bytes)?;
        Ok(Self {
            identity: owner.clone(),
            _charge: charge,
        })
    }
}
/// The caller holds an admitted tuple/header through this reducer's lifetime.
/// It groups by FULL identity, never hash. Only at most two claimant identities
/// are retained; repeated evidence does not grow a map or list. All business
/// conflicts remain provisional until successful tuple EOF and overall EOF.
pub struct UniqueTupleReducer<'a, 'r> {
    tuple: &'a UniqueIdentity,
    initial_owner: Option<&'a RecordIdentity>,
    reservation: &'r QueueReservation,
    released: bool,
    claimant: Option<RetainedOwner<'r>>,
    administrative_claimant: Option<RetainedOwner<'r>>,
    administrative_collision: bool,
    final_collision: bool,
    poisoned: bool,
}
impl<'a, 'r> UniqueTupleReducer<'a, 'r> {
    pub fn new(
        tuple: &'a UniqueIdentity,
        initial_loaded: bool,
        initial_owner: Option<&'a RecordIdentity>,
        reservation: &'r QueueReservation,
    ) -> QueueResult<Self> {
        if !initial_loaded
            || initial_owner.is_some_and(|owner| owner.collection != tuple.collection)
        {
            return Err(unknown("unique tuple initial authority invalid"));
        }
        Ok(Self {
            tuple,
            initial_owner,
            reservation,
            released: false,
            claimant: None,
            administrative_claimant: None,
            administrative_collision: false,
            final_collision: false,
            poisoned: false,
        })
    }
    pub fn observe(&mut self, evidence: UniqueEvidence<'_>) -> QueueResult<()> {
        if self.poisoned {
            return Err(unknown("unique tuple reducer poisoned"));
        }
        self.poisoned = true;
        let result = self.observe_checked(evidence);
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    fn observe_checked(&mut self, evidence: UniqueEvidence<'_>) -> QueueResult<()> {
        if evidence.tuple != self.tuple
            || !evidence.initial_loaded
            || evidence.initial_owner != self.initial_owner
        {
            return Err(unknown("contradictory unique tuple initial authority"));
        }
        match evidence.effect {
            UniqueEvidenceEffect::Observe => {}
            UniqueEvidenceEffect::Release(owner) => {
                if self.initial_owner != Some(owner) || owner.collection != self.tuple.collection {
                    return Err(unknown("released tuple lacks exact initial owner"));
                }
                self.released = true;
            }
            UniqueEvidenceEffect::Claim { owner, origin } => {
                if owner.collection != self.tuple.collection
                    || (origin == UniqueClaimOrigin::Administrative && owner.collection.is_some())
                {
                    return Err(unknown("unique claimant collection mismatch"));
                }
                if let Some(first) = &self.claimant {
                    self.final_collision |= first.identity != *owner;
                } else {
                    self.claimant = Some(RetainedOwner::copy(owner, self.reservation)?);
                }
                if origin == UniqueClaimOrigin::Administrative {
                    if let Some(first) = &self.administrative_claimant {
                        self.administrative_collision |= first.identity != *owner;
                    } else {
                        self.administrative_claimant =
                            Some(RetainedOwner::copy(owner, self.reservation)?);
                    }
                }
            }
        }
        Ok(())
    }
    /// Call only after the source successfully completes this full tuple group.
    /// The driver must also complete every remaining group before publication.
    pub fn finish(self) -> QueueResult<UniqueTupleDecision> {
        self.finish_with(|_| Ok(()))
    }
    /// Observe the final owner synchronously while retained claimant guards are
    /// alive. Collisions return their existing decision without invoking the
    /// callback; None means genuine final nonownership, never a collision.
    /// The caller retains initial-owner/tuple admission, and all output remains
    /// provisional until every tuple and the complete source finish successfully.
    pub fn finish_with(
        self,
        callback: impl FnOnce(Option<&RecordIdentity>) -> QueueResult<()>,
    ) -> QueueResult<UniqueTupleDecision> {
        if self.poisoned {
            return Err(unknown("poisoned unique tuple cannot finish"));
        }
        let blocked_by_initial = self.claimant.as_ref().is_some_and(|claimant| {
            self.initial_owner
                .is_some_and(|owner| *owner != claimant.identity && !self.released)
        });
        let decision = UniqueTupleDecision {
            administrative_collision: self.administrative_collision,
            final_collision: self.final_collision || blocked_by_initial,
        };
        if !decision.administrative_collision && !decision.final_collision {
            let owner = self
                .claimant
                .as_ref()
                .map(|claimant| &claimant.identity)
                .or(if self.released {
                    None
                } else {
                    self.initial_owner
                });
            callback(owner)?;
        }
        Ok(decision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue_admission::QueueAdmission;
    use crate::queue_fields::UniqueFieldEffects;
    use crate::queue_planner::RecordSnapshot;
    use fireweed_core::queue_contract::Rejection;
    use std::collections::{BTreeMap, BTreeSet};
    fn owner(key: &str) -> RecordIdentity {
        RecordIdentity {
            collection: None,
            key: key.into(),
        }
    }
    fn tuple() -> UniqueIdentity {
        UniqueIdentity {
            collection: None,
            index: "unique".into(),
            full_sort_key: vec![0, 255, 0],
        }
    }
    #[test]
    fn streaming_combined_decisions_match_unique_effect_oracle() {
        let tuple = tuple();
        let a = owner("a");
        let b = owner("b");
        let c = owner("c");
        for initial in [None, Some(&a)] {
            for release in [false, true] {
                if release && initial.is_none() {
                    continue;
                }
                for claims in [
                    vec![],
                    vec![&a],
                    vec![&b],
                    vec![&b, &b],
                    vec![&b, &c],
                    vec![&a, &b],
                ] {
                    for release_first in [false, true] {
                        let admission = QueueAdmission::new(1, 65536).unwrap();
                        let job = admission.reserve(0).unwrap();
                        let mut reducer =
                            UniqueTupleReducer::new(&tuple, true, initial, &job).unwrap();
                        let observe_release = |reducer: &mut UniqueTupleReducer<'_, '_>| {
                            reducer
                                .observe(UniqueEvidence {
                                    tuple: &tuple,
                                    initial_loaded: true,
                                    initial_owner: initial,
                                    effect: UniqueEvidenceEffect::Release(&a),
                                })
                                .unwrap()
                        };
                        if release && release_first {
                            observe_release(&mut reducer);
                        }
                        for claimant in &claims {
                            reducer
                                .observe(UniqueEvidence {
                                    tuple: &tuple,
                                    initial_loaded: true,
                                    initial_owner: initial,
                                    effect: UniqueEvidenceEffect::Claim {
                                        owner: claimant,
                                        origin: UniqueClaimOrigin::Ordinary,
                                    },
                                })
                                .unwrap();
                        }
                        if release && !release_first {
                            observe_release(&mut reducer);
                        }
                        let decision = reducer.finish().unwrap();
                        let effects = UniqueFieldEffects {
                            releases: if release {
                                BTreeMap::from([(tuple.clone(), a.clone())])
                            } else {
                                BTreeMap::new()
                            },
                            claims: claims
                                .iter()
                                .map(|owner| (tuple.clone(), (*owner).clone()))
                                .collect(),
                        };
                        let image = RecordSnapshot {
                            unique_probes_loaded: BTreeSet::from([tuple.clone()]),
                            unique_owners: initial
                                .map(|owner| BTreeMap::from([(tuple.clone(), owner.clone())]))
                                .unwrap_or_default(),
                            ..Default::default()
                        };
                        assert_eq!(
                            decision.final_collision,
                            effects.validate(&image).unwrap() == Err(Rejection::UniqueConflict)
                        );
                        assert!(!decision.administrative_collision);
                        assert_eq!(admission.usage().unwrap().1, 0);
                    }
                }
            }
        }
    }
    #[test]
    fn administrative_collision_is_distinct_and_late_corruption_poisoned() {
        let tuple = tuple();
        let a = owner("a");
        let b = owner("b");
        let admission = QueueAdmission::new(1, 65536).unwrap();
        let job = admission.reserve(0).unwrap();
        let mut reducer = UniqueTupleReducer::new(&tuple, true, None, &job).unwrap();
        for (owner, origin) in [
            (&a, UniqueClaimOrigin::Ordinary),
            (&b, UniqueClaimOrigin::Administrative),
        ] {
            reducer
                .observe(UniqueEvidence {
                    tuple: &tuple,
                    initial_loaded: true,
                    initial_owner: None,
                    effect: UniqueEvidenceEffect::Claim { owner, origin },
                })
                .unwrap();
        }
        assert_eq!(
            reducer.finish().unwrap(),
            UniqueTupleDecision {
                administrative_collision: false,
                final_collision: true
            }
        );
        let mut reducer = UniqueTupleReducer::new(&tuple, true, None, &job).unwrap();
        for owner in [&a, &b] {
            reducer
                .observe(UniqueEvidence {
                    tuple: &tuple,
                    initial_loaded: true,
                    initial_owner: None,
                    effect: UniqueEvidenceEffect::Claim {
                        owner,
                        origin: UniqueClaimOrigin::Administrative,
                    },
                })
                .unwrap();
        }
        assert_eq!(
            reducer.finish().unwrap(),
            UniqueTupleDecision {
                administrative_collision: true,
                final_collision: true
            }
        );
        let mut reducer = UniqueTupleReducer::new(&tuple, true, None, &job).unwrap();
        for owner in [&a, &b] {
            reducer
                .observe(UniqueEvidence {
                    tuple: &tuple,
                    initial_loaded: true,
                    initial_owner: None,
                    effect: UniqueEvidenceEffect::Claim {
                        owner,
                        origin: UniqueClaimOrigin::Administrative,
                    },
                })
                .unwrap();
        }
        assert!(
            reducer
                .observe(UniqueEvidence {
                    tuple: &tuple,
                    initial_loaded: true,
                    initial_owner: Some(&a),
                    effect: UniqueEvidenceEffect::Release(&a)
                })
                .is_err()
        );
        assert!(reducer.finish().is_err());
        assert_eq!(admission.usage().unwrap().1, 0);
    }
    #[test]
    fn repeated_large_exact_tuple_retains_constant_owner_admission() {
        let mut tuple = tuple();
        tuple.full_sort_key = vec![7; 1 << 20];
        let owner = owner("owner");
        let admission = QueueAdmission::new(1, 8 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let tuple_charge = job
            .buffer((tuple.full_sort_key.len() as u64) * 4 + 4096)
            .unwrap();
        let baseline = admission.usage().unwrap().1;
        let mut reducer = UniqueTupleReducer::new(&tuple, true, None, &job).unwrap();
        let mut usage = None;
        for _ in 0..5001 {
            reducer
                .observe(UniqueEvidence {
                    tuple: &tuple,
                    initial_loaded: true,
                    initial_owner: None,
                    effect: UniqueEvidenceEffect::Claim {
                        owner: &owner,
                        origin: UniqueClaimOrigin::Administrative,
                    },
                })
                .unwrap();
            let current = admission.usage().unwrap().1;
            assert_eq!(*usage.get_or_insert(current), current);
        }
        assert_eq!(
            reducer.finish().unwrap(),
            UniqueTupleDecision {
                administrative_collision: false,
                final_collision: false
            }
        );
        assert_eq!(admission.usage().unwrap().1, baseline);
        drop(tuple_charge);
        assert_eq!(admission.usage().unwrap().1, 0);
    }
    #[test]
    fn final_owner_callback_preserves_observation_release_reclaim_and_order() {
        let tuple = tuple();
        let a = owner("a");
        let b = owner("b");
        let admission = QueueAdmission::new(1, 65536).unwrap();
        let job = admission.reserve(0).unwrap();
        for (initial, release, claimant, expected) in [
            (None, false, None, None),
            (Some(&a), false, None, Some(&a)),
            (Some(&a), true, None, None),
            (Some(&a), false, Some(&a), Some(&a)),
            (Some(&a), true, Some(&a), Some(&a)),
            (Some(&a), true, Some(&b), Some(&b)),
            (None, false, Some(&b), Some(&b)),
        ] {
            for release_first in [false, true] {
                let mut reducer = UniqueTupleReducer::new(&tuple, true, initial, &job).unwrap();
                reducer
                    .observe(UniqueEvidence {
                        tuple: &tuple,
                        initial_loaded: true,
                        initial_owner: initial,
                        effect: UniqueEvidenceEffect::Observe,
                    })
                    .unwrap();
                for is_release in [release_first, !release_first] {
                    let effect = if is_release {
                        if !release {
                            continue;
                        }
                        UniqueEvidenceEffect::Release(initial.unwrap())
                    } else {
                        let Some(owner) = claimant else {
                            continue;
                        };
                        UniqueEvidenceEffect::Claim {
                            owner,
                            origin: UniqueClaimOrigin::Ordinary,
                        }
                    };
                    reducer
                        .observe(UniqueEvidence {
                            tuple: &tuple,
                            initial_loaded: true,
                            initial_owner: initial,
                            effect,
                        })
                        .unwrap();
                }
                let mut seen = Vec::new();
                let result = reducer
                    .finish_with(|owner| {
                        seen.push(owner.cloned());
                        Ok(())
                    })
                    .unwrap();
                assert!(!result.administrative_collision && !result.final_collision);
                assert_eq!(seen, vec![expected.cloned()]);
                assert_eq!(admission.usage().unwrap().1, 0);
            }
        }
    }
    #[test]
    fn collisions_and_poison_never_call_final_owner_callback() {
        let tuple = tuple();
        let a = owner("a");
        let b = owner("b");
        let admission = QueueAdmission::new(1, 65536).unwrap();
        let job = admission.reserve(0).unwrap();
        // Initial-owner conflict, administrative sibling collision, and an
        // ordinary sibling collision with no initial owner are distinct paths.
        for mode in 0..3 {
            let administrative = mode == 1;
            let initial = (mode == 0).then_some(&a);
            let mut reducer = UniqueTupleReducer::new(&tuple, true, initial, &job).unwrap();
            let origin = if administrative {
                UniqueClaimOrigin::Administrative
            } else {
                UniqueClaimOrigin::Ordinary
            };
            for owner in if mode == 0 { vec![&b] } else { vec![&a, &b] } {
                reducer
                    .observe(UniqueEvidence {
                        tuple: &tuple,
                        initial_loaded: true,
                        initial_owner: initial,
                        effect: UniqueEvidenceEffect::Claim { owner, origin },
                    })
                    .unwrap();
            }
            let result = reducer
                .finish_with(|_| panic!("colliding owner exposed"))
                .unwrap();
            assert!(result.final_collision);
            assert_eq!(result.administrative_collision, administrative);
            assert_eq!(admission.usage().unwrap().1, 0);
        }
        let mut reducer = UniqueTupleReducer::new(&tuple, true, Some(&a), &job).unwrap();
        assert!(
            reducer
                .observe(UniqueEvidence {
                    tuple: &tuple,
                    initial_loaded: true,
                    initial_owner: Some(&a),
                    effect: UniqueEvidenceEffect::Release(&b)
                })
                .is_err()
        );
        assert!(
            reducer
                .finish_with(|_| panic!("poisoned owner exposed"))
                .is_err()
        );
        assert_eq!(admission.usage().unwrap().1, 0);
    }
    #[test]
    fn final_owner_callback_retains_credit_through_success_error_and_unwind() {
        let tuple = tuple();
        let claimant = owner(&"full-owner\0β".repeat(1024));
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        for mode in 0..3 {
            let mut reducer = UniqueTupleReducer::new(&tuple, true, None, &job).unwrap();
            reducer
                .observe(UniqueEvidence {
                    tuple: &tuple,
                    initial_loaded: true,
                    initial_owner: None,
                    effect: UniqueEvidenceEffect::Claim {
                        owner: &claimant,
                        origin: UniqueClaimOrigin::Ordinary,
                    },
                })
                .unwrap();
            let retained = admission.usage().unwrap().1;
            assert!(retained > claimant.key.len() as u64);
            let filler = job.buffer((1 << 20) - retained).unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                reducer.finish_with(|owner| {
                    assert_eq!(admission.usage().unwrap().1, 1 << 20);
                    let owner = owner.unwrap();
                    assert_eq!(owner, &claimant);
                    assert_ne!(owner.key.as_ptr(), claimant.key.as_ptr());
                    match mode {
                        0 => Ok(()),
                        1 => Err(unknown("callback stopped")),
                        _ => panic!("callback unwind"),
                    }
                })
            }));
            match mode {
                0 => {
                    assert!(!result.unwrap().unwrap().final_collision);
                }
                1 => {
                    assert!(
                        matches!(result.unwrap(), Err(QueueError::OutcomeUnknown(ref message)) if message == "callback stopped")
                    );
                }
                _ => {
                    let panic = result.unwrap_err();
                    assert_eq!(
                        panic.downcast_ref::<&str>().copied(),
                        Some("callback unwind")
                    );
                }
            }
            assert_eq!(admission.usage().unwrap().1, (1 << 20) - retained);
            drop(filler);
            assert_eq!(admission.usage().unwrap().1, 0);
        }
    }
    #[test]
    fn missing_authority_wrong_tuple_release_and_capacity_fail_closed() {
        let tuple = tuple();
        let owner = owner("owner");
        let admission = QueueAdmission::new(1, 64).unwrap();
        let job = admission.reserve(0).unwrap();
        assert!(UniqueTupleReducer::new(&tuple, false, None, &job).is_err());
        let mut different = tuple.clone();
        different.index = "another-index".into();
        let mut reducer = UniqueTupleReducer::new(&tuple, true, None, &job).unwrap();
        assert!(
            reducer
                .observe(UniqueEvidence {
                    tuple: &different,
                    initial_loaded: true,
                    initial_owner: None,
                    effect: UniqueEvidenceEffect::Release(&owner)
                })
                .is_err()
        );
        assert!(reducer.finish().is_err());
        let mut reducer = UniqueTupleReducer::new(&tuple, true, None, &job).unwrap();
        assert!(
            reducer
                .observe(UniqueEvidence {
                    tuple: &tuple,
                    initial_loaded: true,
                    initial_owner: None,
                    effect: UniqueEvidenceEffect::Release(&owner)
                })
                .is_err()
        );
        assert!(reducer.finish().is_err());
        let mut reducer = UniqueTupleReducer::new(&tuple, true, None, &job).unwrap();
        assert!(
            reducer
                .observe(UniqueEvidence {
                    tuple: &tuple,
                    initial_loaded: true,
                    initial_owner: None,
                    effect: UniqueEvidenceEffect::Claim {
                        owner: &owner,
                        origin: UniqueClaimOrigin::Ordinary
                    }
                })
                .is_err()
        );
        assert!(reducer.finish().is_err());
        assert_eq!(admission.usage().unwrap().1, 0);
    }
}
