//! Disposable full-identity sets. These sets carry no source-presence, version,
//! range-completion or selection authority. The engine must supply that bridge.
use super::{NativeScratchDatabase, unknown};
use fireweed_core::queue_contract::{QueueError, QueueResult};
use fireweed_engine::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_source_index::{QueueSourceKey, QueueSourceSelectionBudget, QueueSourceSelectionWork},
};
use fireweed_queue_spool::{ScratchDiskQuota, ScratchRoot};
use std::sync::Arc;
use turso::Value;

const SOURCE_SCHEMA: &[&str] =
    &["CREATE TABLE source_sets(node INTEGER NOT NULL,key BLOB NOT NULL,PRIMARY KEY(node,key))"];

const INSERT: &str = "INSERT INTO source_sets(node,key) VALUES(?1,?2)";
const CONTAINS: &str = "SELECT 1 FROM source_sets WHERE node=?1 AND key=?2";
const NEXT_LEN: &str =
    "SELECT length(key) FROM source_sets WHERE node=?1 AND key>?2 ORDER BY key LIMIT 1";
const NEXT_KEY: &str = "SELECT key FROM source_sets WHERE node=?1 AND key>?2 ORDER BY key LIMIT 1";

struct Scope {
    _memory: OwnedQueueBufferCharge,
}
/// A mutable leaf cannot be cloned. Sealing consumes it; immutable operands
/// never share a mutable builder, and algebra always creates a fresh output.
pub struct SourceSetBuilder {
    scope: Arc<Scope>,
    node: i64,
    rows: u64,
    _memory: OwnedQueueBufferCharge,
}
pub struct SourceCandidateSet(SourceSetBuilder);
/// The full canonical Record key retains admission independently of the database.
/// It is an internal identity, not evidence that a source record exists.
pub struct SourceCandidateIdentity {
    scope: Arc<Scope>,
    key: Vec<u8>,
    _memory: OwnedQueueBufferCharge,
}
impl SourceCandidateIdentity {
    pub fn encoded_identity(&self) -> &[u8] {
        &self.key
    }
}
impl SourceCandidateSet {
    pub fn len(&self) -> u64 {
        self.0.rows
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
/// Use inside an owned physical worker, never a nested runtime. Taking `owner`
/// BEFORE every await means cancellation retires the actual database; handles
/// retain only a funded identity token. No handle owns or revives the database.
/// Existing SDK memory allowances remain qualifier hypotheses, not hard RAM proof.
pub struct SourceCandidateScratch {
    owner: Option<NativeScratchDatabase>,
    scope: Arc<Scope>,
    job: Arc<QueueReservation>,
    budget: Arc<QueueSourceSelectionBudget>,
    next_node: i64,
    #[cfg(test)]
    pause_after_write: Option<Arc<std::sync::atomic::AtomicBool>>,
}
impl SourceCandidateScratch {
    pub async fn open(
        root: Arc<ScratchRoot>,
        quota: Arc<ScratchDiskQuota>,
        job: Arc<QueueReservation>,
        budget: Arc<QueueSourceSelectionBudget>,
    ) -> QueueResult<Self> {
        budget.remaining(&job)?;
        let memory = job.owned_buffer(
            (std::mem::size_of::<Self>()
                + std::mem::size_of::<Scope>()
                + 2 * std::mem::size_of::<usize>()) as u64,
        )?;
        // Fixed schema/PRAGMA setup, including the two scalar readbacks and
        // spare checkpoint capacity. No data-dependent traversal occurs here.
        budget.grant(
            &job,
            QueueSourceSelectionWork {
                scratch_steps: 32,
                ..Default::default()
            },
        )?;
        let db = NativeScratchDatabase::open_with_schema(root, quota, &job, SOURCE_SCHEMA).await?;
        Ok(Self {
            owner: Some(db),
            scope: Arc::new(Scope { _memory: memory }),
            job,
            budget,
            next_node: 0,
            #[cfg(test)]
            pause_after_write: None,
        })
    }
    fn ready(&self) -> QueueResult<()> {
        if self.owner.is_none() {
            return Err(unknown());
        }
        Ok(())
    }
    fn check(&self, scope: &Arc<Scope>) -> QueueResult<()> {
        if !Arc::ptr_eq(&self.scope, scope) {
            return Err(unknown());
        }
        self.ready()
    }
    pub fn begin_set(&mut self) -> QueueResult<SourceSetBuilder> {
        self.ready()?;
        let node = self.next_node.checked_add(1).ok_or(QueueError::TooLarge)?;
        let memory = self
            .job
            .owned_buffer(std::mem::size_of::<SourceSetBuilder>() as u64)?;
        self.next_node = node;
        Ok(SourceSetBuilder {
            scope: self.scope.clone(),
            node,
            rows: 0,
            _memory: memory,
        })
    }
    pub fn seal(&self, builder: SourceSetBuilder) -> QueueResult<SourceCandidateSet> {
        self.check(&builder.scope)?;
        Ok(SourceCandidateSet(builder))
    }
    // Every SQL call is prepaid, including terminal probes and duplicate checks.
    // execute may issue one WAL checkpoint, so charge two steps conservatively.
    async fn one(
        &self,
        db: &mut NativeScratchDatabase,
        sql: &str,
        bytes: u64,
        params: impl FnOnce() -> Vec<Value>,
    ) -> QueueResult<Option<Vec<Value>>> {
        self.budget.grant(
            &self.job,
            QueueSourceSelectionWork {
                rows: 1,
                copied_bytes: bytes,
                scratch_steps: 1,
            },
        )?;
        db.admit_key(usize::try_from(bytes).map_err(|_| QueueError::TooLarge)?)?;
        db.one(sql, params()).await
    }
    async fn insert_key(
        &self,
        db: &mut NativeScratchDatabase,
        builder: &mut SourceSetBuilder,
        key: &[u8],
    ) -> QueueResult<()> {
        let node = builder.node;
        let bytes = u64::try_from(key.len()).map_err(|_| QueueError::TooLarge)?;
        let found = self
            .one(db, CONTAINS, bytes, || {
                vec![Value::Integer(node), Value::Blob(key.to_vec())]
            })
            .await?;
        if let Some(row) = found {
            if row.as_slice() != [Value::Integer(1)] {
                return Err(unknown());
            }
            return Ok(());
        }
        let rows = builder.rows.checked_add(1).ok_or(QueueError::TooLarge)?;
        self.budget.grant(
            &self.job,
            QueueSourceSelectionWork {
                rows: 1,
                copied_bytes: bytes,
                scratch_steps: 2,
            },
        )?;
        db.admit_key(key.len())?;
        db.execute(
            INSERT,
            vec![Value::Integer(node), Value::Blob(key.to_vec())],
        )
        .await?;
        #[cfg(test)]
        if let Some(paused) = &self.pause_after_write {
            paused.store(true, std::sync::atomic::Ordering::Release);
            // Actual SQL has completed; this hook tests retirement after a
            // partial set write, not cancellation of pending SDK I/O.
            std::future::pending::<()>().await;
        }
        builder.rows = rows;
        Ok(())
    }
    pub async fn insert(
        &mut self,
        builder: &mut SourceSetBuilder,
        collection: Option<&str>,
        key: &str,
    ) -> QueueResult<()> {
        self.check(&builder.scope)?;
        let identity = QueueSourceKey::Record { collection, key };
        let bytes = identity.encoded_len()?;
        let charge = self.job.owned_buffer(bytes as u64)?;
        self.budget.grant(
            &self.job,
            QueueSourceSelectionWork {
                copied_bytes: bytes as u64,
                ..Default::default()
            },
        )?;
        let key = identity.encode()?;
        let mut db = self.owner.take().ok_or_else(unknown)?;
        self.insert_key(&mut db, builder, &key).await?;
        drop(key);
        drop(charge);
        self.owner = Some(db);
        Ok(())
    }
    pub async fn next(
        &mut self,
        set: &SourceCandidateSet,
        after: Option<&SourceCandidateIdentity>,
    ) -> QueueResult<Option<SourceCandidateIdentity>> {
        self.check(&set.0.scope)?;
        if let Some(after) = after {
            self.check(&after.scope)?;
        }
        let key = after.map_or(&[][..], |row| row.key.as_slice());
        let mut db = self.owner.take().ok_or_else(unknown)?;
        let node = set.0.node;
        let length = self
            .one(&mut db, NEXT_LEN, key.len() as u64, || {
                vec![Value::Integer(node), Value::Blob(key.to_vec())]
            })
            .await?;
        let Some(length) = length else {
            self.owner = Some(db);
            return Ok(None);
        };
        let [Value::Integer(length)] = length.as_slice() else {
            return Err(unknown());
        };
        let length = usize::try_from(*length).map_err(|_| unknown())?;
        if length == 0 {
            return Err(unknown());
        }
        let memory = self.job.owned_buffer(
            (length as u64)
                .checked_add(std::mem::size_of::<SourceCandidateIdentity>() as u64)
                .ok_or(QueueError::TooLarge)?,
        )?;
        let bytes = (key.len() as u64)
            .checked_add(length as u64)
            .ok_or(QueueError::TooLarge)?;
        let row = self
            .one(&mut db, NEXT_KEY, bytes, || {
                vec![Value::Integer(node), Value::Blob(key.to_vec())]
            })
            .await?;
        let Some(mut row) = row else {
            return Err(unknown());
        };
        if row.len() != 1 {
            return Err(unknown());
        }
        let Some(Value::Blob(value)) = row.pop() else {
            return Err(unknown());
        };
        if value.len() != length || value.as_slice() <= key || value.capacity() > length {
            return Err(unknown());
        }
        self.owner = Some(db);
        Ok(Some(SourceCandidateIdentity {
            scope: self.scope.clone(),
            key: value,
            _memory: memory,
        }))
    }
    pub async fn union(
        &mut self,
        left: &SourceCandidateSet,
        right: &SourceCandidateSet,
    ) -> QueueResult<SourceCandidateSet> {
        self.binary(left, right, Operation::Union).await
    }
    pub async fn intersection(
        &mut self,
        left: &SourceCandidateSet,
        right: &SourceCandidateSet,
    ) -> QueueResult<SourceCandidateSet> {
        self.binary(left, right, Operation::Intersection).await
    }
    pub async fn difference(
        &mut self,
        left: &SourceCandidateSet,
        right: &SourceCandidateSet,
    ) -> QueueResult<SourceCandidateSet> {
        self.binary(left, right, Operation::Difference).await
    }
    async fn binary(
        &mut self,
        left: &SourceCandidateSet,
        right: &SourceCandidateSet,
        operation: Operation,
    ) -> QueueResult<SourceCandidateSet> {
        self.check(&left.0.scope)?;
        self.check(&right.0.scope)?;
        let mut output = self.begin_set()?;
        for set in [left, right]
            .into_iter()
            .take(if matches!(operation, Operation::Union) {
                2
            } else {
                1
            })
        {
            let mut after = None;
            loop {
                let Some(row) = self.next(set, after.as_ref()).await? else {
                    break;
                };
                let mut db = self.owner.take().ok_or_else(unknown)?;
                let include = if matches!(operation, Operation::Union) {
                    true
                } else {
                    let found = self
                        .one(&mut db, CONTAINS, row.key.len() as u64, || {
                            vec![Value::Integer(right.0.node), Value::Blob(row.key.clone())]
                        })
                        .await?;
                    let member = match found.as_deref() {
                        None => false,
                        Some([Value::Integer(1)]) => true,
                        _ => return Err(unknown()),
                    };
                    member == matches!(operation, Operation::Intersection)
                };
                if include {
                    self.insert_key(&mut db, &mut output, &row.key).await?;
                }
                self.owner = Some(db);
                after = Some(row);
            }
        }
        self.seal(output)
    }
}
#[derive(Clone, Copy)]
enum Operation {
    Union,
    Intersection,
    Difference,
}

#[cfg(test)]
mod tests;
