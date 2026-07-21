//! Reusable durable-boundary vocabulary for SP-02/SP-03/SP-07 adapter tests.
//!
//! This module has no runtime state and is not used by production construction. It gives integration tests
//! one shared mapping onto the existing production fault taxonomy without depending on a private test file.

use crate::segmented::FaultCutPoint;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SimulationBlobPhase {
    /// Durable helper objects that are not themselves a protocol cut (recovery-index nodes, initialization
    /// markers, snapshots, and similar support records). Fault scripts for an authoritative head must never
    /// be consumed by one of these earlier writes.
    Auxiliary,
    Segment,
    ManifestCandidate,
    ManifestHead,
    EpochCandidate,
    EpochHead,
    Floor,
    Horizon,
    Delete,
    ListPage,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SimulationDurableCut {
    BeforeSegmentWrite,
    AfterSegmentWriteBeforeManifest,
    AfterManifestCandidateBeforeHead,
    AfterManifestBeforeAck,
    DuringOwnerReassignment,
    DuringSegmentExpiry,
}

pub fn production_fault_cut(cut: SimulationDurableCut) -> FaultCutPoint {
    match cut {
        SimulationDurableCut::BeforeSegmentWrite => FaultCutPoint::BeforeSegmentWrite,
        SimulationDurableCut::AfterSegmentWriteBeforeManifest => {
            FaultCutPoint::AfterSegmentWriteBeforeManifest
        }
        SimulationDurableCut::AfterManifestCandidateBeforeHead => {
            FaultCutPoint::AfterManifestCandidateBeforeHead
        }
        SimulationDurableCut::AfterManifestBeforeAck => FaultCutPoint::AfterManifestBeforeAck,
        SimulationDurableCut::DuringOwnerReassignment => FaultCutPoint::DuringOwnerReassignment,
        SimulationDurableCut::DuringSegmentExpiry => FaultCutPoint::DuringSegmentExpiry,
    }
}
