//! Engine-owned correspondence between checked resolved records and source-map
//! facts. Storage qualification and exact-cut ownership remain adapter duties.
mod reference;
pub use reference::QueueSourceSpan;

mod scope;
pub use scope::{QueueSourceCut, QueueSourcePoint};

mod intrinsic;
pub use intrinsic::{
    QueueSourceWorkIndex, QueueSourceWorkPosting, QueueSourceWorkPrefix, QueueSourceWorkValue,
};

mod key;
pub use key::{
    QueueSourceKey, live_keys_prefix, live_keys_prefix_len, prefix_successor, record_fields_prefix,
    record_fields_prefix_len,
};

mod cell;
pub use cell::{
    QueueSourceGenesis, QueueSourceLive, QueueSourceOriginal, QueueSourcePayload, QueueSourceRecord,
};

mod genesis;
pub use genesis::load_source_genesis;
mod header;
pub use header::{QueueSourceHeader, load_source_header};
mod original;
pub use original::{QueueSourceOriginalEvidence, load_source_original};

mod compile;
mod edits;
pub use compile::{QueueSourceCompileLimits, QueueSourceCompiled, compile_genesis};

mod read;
pub use read::QueueSourceReader;

mod fields;
pub use fields::{QueueSourceFields, load_command_fields, source_field_value};

mod predecessor;
pub use predecessor::{
    QueueSourceAggregateAccumulator, QueueSourceAggregateEdits, QueueSourceCommand,
    QueueSourceIntrinsicAccumulator, QueueSourceIntrinsicEdits, QueueSourceNamedEffect,
    QueueSourceOriginalEdits, QueueSourcePredecessor, QueueSourcePublicationContext,
    QueueSourceRecordEditAccumulator, QueueSourceRecordEdits, QueueSourceRecordTransition,
    QueueSourceUniqueOwnerAccumulator, QueueSourceUniqueOwnerEdits, compile_publication,
};

mod group;
pub use group::{
    QueueSourceBatchPlan, QueueSourceBatchPreimage, QueueSourceBatchPublication,
    QueueSourceGroupEncodingLimits, QueueSourceGroupPlan, QueueSourceGroupPreimage,
    QueueSourceGroupPublication, load_batch_preimage, load_group_preimage,
};
