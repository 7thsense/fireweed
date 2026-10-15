//! Engine-owned correspondence between checked resolved records and source-map
//! facts. Storage qualification and exact-cut ownership remain adapter duties.
mod reference;
pub use reference::QueueSourceSpan;

mod scope;
pub use scope::{QueueSourceCut, QueueSourcePoint};

mod key;
pub use key::{QueueSourceKey, prefix_successor, record_fields_prefix, record_fields_prefix_len};

mod cell;
pub use cell::{
    QueueSourceGenesis, QueueSourceLive, QueueSourceOriginal, QueueSourcePayload, QueueSourceRecord,
};

mod compile;
mod edits;
pub use compile::{QueueSourceCompileLimits, QueueSourceCompiled, compile_genesis};

mod read;
pub use read::QueueSourceReader;

mod fields;
pub use fields::{QueueSourceFields, load_command_fields, source_field_value};

mod predecessor;
pub use predecessor::{
    QueueSourceCommand, QueueSourceNamedEffect, QueueSourcePredecessor,
    QueueSourcePublicationContext, QueueSourceRecordEditAccumulator, QueueSourceRecordEdits,
    QueueSourceRecordTransition, QueueSourceUniqueOwnerAccumulator, QueueSourceUniqueOwnerEdits,
};
