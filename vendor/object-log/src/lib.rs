//! A buffered, multiplexing append log over pluggable object storage.
//!
//! object-log stores an ordered, offset-addressed log as immutable objects in any
//! [`BlobStore`] (memory, local filesystem, or — behind a feature — S3). It deals
//! only in **opaque payload bytes**: it knows nothing about record formats, Kafka,
//! or brokers. Three pieces:
//!
//! - [`BlobStore`] — the storage *port*: durable-on-return `put`, `get`,
//!   `get_range`, `list`, `delete`. Adapters [`MemoryBlobStore`] and
//!   [`LocalBlobStore`] ship in-crate.
//! - [`Sequencer`] — the *sequencing seam*: assigns offsets to durably-stored
//!   batches and owns the offset→location index. [`InMemorySequencer`] ships
//!   in-crate; a consumer plugs its own (e.g. a Kafka coordinator) and the engine
//!   forwards each sequencer's [`Sequencer::Meta`] uninterpreted.
//! - [`LogEngine`] — buffers and group-commits many batches into one object, PUTs
//!   it durably, then calls the sequencer, so PUT count is decoupled from produce
//!   count. [`LogEngine::produce`] resolves at a requested [`Durability`].
//!
//! # Example
//!
//! ```
//! use object_log::{Durability, FlushConfig, InMemorySequencer, LogEngine, MemoryBlobStore, PartitionKey};
//! use bytes::Bytes;
//! use std::sync::Arc;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let engine = LogEngine::new(
//!     Arc::new(MemoryBlobStore::new()),
//!     Arc::new(InMemorySequencer::new()),
//!     FlushConfig::default(),
//!     "log/",
//! );
//! let p = PartitionKey("events-0".into());
//!
//! let out = engine
//!     .produce(p.clone(), Bytes::from_static(b"hello"), 1, (), Durability::Sequenced)
//!     .await
//!     .unwrap();
//! assert_eq!(out.base_offset, Some(0));
//!
//! let batches = engine.fetch(&p, 0, 1024).await.unwrap();
//! assert_eq!(batches[0].payload, "hello");
//! # });
//! ```
#![deny(missing_docs)]

mod blob;
mod budget;
mod engine;
mod error;
mod manifest_sequencer;
#[cfg(feature = "s3")]
mod s3;
mod sequencer;

pub use blob::{BlobStore, LocalBlobStore, MediaOpStats, MemoryBlobStore};
pub use budget::{BudgetConfig, BudgetMode, EffectiveKnob, EffectiveReason, PipelineSnapshot};
pub use engine::{
    AppendOutcome, BufferStats, Durability, FetchedBatch, FlushConfig, LogEngine, reap_orphans,
};
pub use error::ObjectLogError;
pub use manifest_sequencer::{IndexSnapshot, ManifestSequencer, PartitionSnapshot};
#[cfg(feature = "s3")]
pub use s3::S3BlobStore;
pub use sequencer::{
    BatchLocation, CommitBatch, CommitOutcome, InMemorySequencer, IndexEntry, PartitionKey,
    Sequencer,
};
