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
    QueueSourceDecodedIdentity, QueueSourceDecodedPosting, QueueSourceDecodedWorkPosting,
    QueueSourceKey, QueueSourcePostingFamily, decode_live_key, live_keys_prefix,
    live_keys_prefix_len, prefix_successor, record_field_name_prefix, record_field_name_prefix_len,
    record_fields_prefix, record_fields_prefix_len,
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
pub use original::{
    QueueSourceOriginalEvidence, QueueSourceOriginalObservation, QueueSourceVerifiedOriginals,
    load_source_original, verify_source_originals,
};

mod compile;
mod edits;
pub use compile::{QueueSourceCompileLimits, QueueSourceCompiled, compile_genesis};

mod read;
pub use read::{QueueSourceReader, QueueSourceRecordReader, QueueSourceStreamingReader};

mod body;
pub use body::{
    QueueSourceVerifiedWorkBodies, QueueSourceWorkBodyEvidence, verify_source_work_bodies,
};

mod state_read;
pub use state_read::load_source_state_records;
mod state_value;
pub use state_value::{
    QueueSourceStateValueEvidence, QueueSourceVerifiedStateValues, verify_source_state_values,
};

mod fields;
pub use fields::{
    QueueSourceFieldSelection, QueueSourceFields, load_command_fields, load_selected_fields,
    source_field_value,
};

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
    QueueSourceAdministrativeAdmission, QueueSourceAdministrativeRead,
    QueueSourceAdministrativeReader, QueueSourceAdministrativeRow, QueueSourceAdministrativeScan,
    QueueSourceAdministrativeScanContext, QueueSourceBatchPlan, QueueSourceBatchPreimage,
    QueueSourceBatchPublication, QueueSourceGroupEncodingLimits, QueueSourceGroupPlan,
    QueueSourceGroupPreimage, QueueSourceGroupPublication, QueueSourceOrdinaryAuthority,
    QueueSourceOrdinaryBinding, admit_administrative_group, load_administrative_ordinary_authority,
    load_batch_preimage, load_group_preimage, open_administrative_source,
};

mod selection;
pub use selection::{QueueSourceScalarRange, QueueSourceScalarRanges, plan_source_scalar_ranges};
