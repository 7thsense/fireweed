//! Linux physical qualifier for disposable native set storage. This is not a
//! QueueStore or a public backend qualification. No authority is attached.
mod io;
use fireweed_core::queue_contract::{QueueError, QueueResult};
use fireweed_engine::queue_admission::{OwnedQueueBufferCharge, QueueReservation};
use fireweed_queue_spool::{ScratchDiskQuota, ScratchRoot};
use io::ScratchIo;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use turso::{Builder, Connection, Database, Value, params_from_iter};

const INSERT_RECORD: &str = "INSERT OR IGNORE INTO records(id,key,version) VALUES(?1,?2,?3)";
const CHECK_RECORD: &str = "SELECT key=?2 AND version=?3 FROM records WHERE id=?1";
const FIND_RECORD: &str = "SELECT 1 FROM records WHERE id=?1";
const FIND_OWNER: &str = "SELECT clause FROM owners WHERE record=?1";
const INSERT_SET: &str = "INSERT OR IGNORE INTO sets(node,record) VALUES(?1,?2)";
const INSERT_EDGE: &str = "INSERT OR IGNORE INTO edges(parent,child) VALUES(?1,?2)";
const UNION: &str = "INSERT OR IGNORE INTO accumulator(node,record) SELECT ?1,?2 WHERE EXISTS(SELECT 1 FROM sets WHERE node=?3 AND record=?2)";
const INTERSECTION: &str = "INSERT OR IGNORE INTO accumulator(node,record) SELECT ?1,?2 WHERE NOT EXISTS(SELECT 1 FROM edges e WHERE e.parent=?1 AND NOT EXISTS(SELECT 1 FROM sets r WHERE r.node=e.child AND r.record=?2))";
const COMPLEMENT: &str = "INSERT OR IGNORE INTO accumulator(node,record) SELECT ?1,?2 WHERE NOT EXISTS(SELECT 1 FROM sets WHERE node=?3 AND record=?2)";
const INSERT_OWNER: &str = "INSERT OR IGNORE INTO owners(record,clause,key,version) SELECT id,?2,key,version FROM records WHERE id=?1";
const NEXT_ACCUMULATOR: &str =
    "SELECT record FROM accumulator WHERE node=?1 AND record>?2 ORDER BY record LIMIT 1";
const NEXT_SET: &str =
    "SELECT record FROM sets WHERE node=?1 AND record>?2 ORDER BY record LIMIT 1";
const NEXT_OWNER: &str = "SELECT record,clause,length(key),version FROM owners INDEXED BY owners_key WHERE key>?1 ORDER BY key LIMIT 1";
const READ_OWNER_KEY: &str = "SELECT key FROM owners WHERE record=?1";
fn unknown() -> QueueError {
    QueueError::OutcomeUnknown("native scratch operation failed".into())
}

/// The physical executor keeps this complete owner inside its admitted job.
/// An abandoned or panicking in-progress method discards the entire owner;
/// an interrupted owner must never be resumed. Fields close SDK resources and
/// files before either memory guard releases. The fixed/key memory allowances
/// are accounting hypotheses pending complete SDK peak correspondence, rather
/// than a claim that cache readback establishes a hard resident-memory bound.
pub struct NativeScratchDatabase {
    connection: Connection,
    _database: Database,
    _io: Arc<ScratchIo>,
    key_charges: Vec<OwnedQueueBufferCharge>,
    key_peak: u64,
    poisoned: AtomicBool,
    statements: AtomicU64,
    _memory: OwnedQueueBufferCharge,
}
/// A selected key retains its same-job capacity after the database closes.
pub struct SelectedScratchRow {
    id: i64,
    clause: i64,
    version: u64,
    key: Vec<u8>,
    _charge: OwnedQueueBufferCharge,
}
impl SelectedScratchRow {
    pub fn id(&self) -> i64 {
        self.id
    }
    pub fn clause(&self) -> i64 {
        self.clause
    }
    pub fn version(&self) -> u64 {
        self.version
    }
    pub fn key(&self) -> &[u8] {
        &self.key
    }
}
impl NativeScratchDatabase {
    /// Call inside an existing owned physical SDK runtime, never nested block_on.
    pub async fn open(
        root: Arc<ScratchRoot>,
        quota: Arc<ScratchDiskQuota>,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Self> {
        let path_bytes = (root.path().as_os_str().len() as u64)
            .checked_mul(16)
            .and_then(|n| n.checked_add(8 << 20))
            .ok_or(QueueError::TooLarge)?;
        // Covers descriptor/map/header/program owners before their allocation.
        // Cache readback and RSS alone are not a proof of a hard RAM ceiling.
        let memory = job.owned_buffer(path_bytes)?;
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = NEXT
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| unknown())?;
        let namespace = root
            .path()
            .join(format!(".native-{}-{id}", std::process::id()))
            .to_str()
            .ok_or_else(|| QueueError::Invalid("native scratch root must be UTF-8".into()))?
            .to_owned();
        let path = format!("{namespace}/main.db");
        let io = ScratchIo::new(root, quota, namespace).map_err(|_| unknown())?;
        let database = Builder::new_local(&path)
            .with_io_impl(io.clone())
            .build()
            .await
            .map_err(|_| unknown())?;
        let connection = database.connect().map_err(|_| unknown())?;
        drop(path);
        let owner = Self {
            connection,
            _database: database,
            _io: io,
            _memory: memory,
            key_charges: Vec::new(),
            key_peak: 0,
            poisoned: AtomicBool::new(false),
            statements: AtomicU64::new(0),
        };
        if owner
            .one("PRAGMA journal_mode=WAL", vec![])
            .await?
            .as_deref()
            != Some(&[Value::Text("wal".into())])
        {
            return Err(unknown());
        }
        for sql in [
            "PRAGMA synchronous=OFF",
            "PRAGMA cache_size=-1024",
            "PRAGMA cache_spill=ON",
            "CREATE TABLE records(id INTEGER PRIMARY KEY,key BLOB NOT NULL,version BLOB NOT NULL)",
            "CREATE UNIQUE INDEX records_key ON records(key)",
            "CREATE TABLE sets(node INTEGER NOT NULL,record INTEGER NOT NULL,PRIMARY KEY(node,record))",
            "CREATE TABLE accumulator(node INTEGER NOT NULL,record INTEGER NOT NULL,PRIMARY KEY(node,record))",
            "CREATE TABLE edges(parent INTEGER NOT NULL,child INTEGER NOT NULL,PRIMARY KEY(parent,child))",
            "CREATE TABLE owners(record INTEGER PRIMARY KEY,clause INTEGER NOT NULL,key BLOB NOT NULL,version BLOB NOT NULL)",
            "CREATE UNIQUE INDEX owners_key ON owners(key)",
        ] {
            owner.execute(sql, vec![]).await?;
        }
        let cache = owner.one("PRAGMA cache_size", vec![]).await?;
        if cache.as_deref() != Some(&[Value::Integer(-1024)]) {
            return Err(unknown());
        }
        let spill = owner.one("PRAGMA cache_spill", vec![]).await?;
        if !matches!(spill.as_deref(), Some([Value::Integer(n)]) if *n != 0) {
            return Err(unknown());
        }
        Ok(owner)
    }
    fn check(&self) -> QueueResult<()> {
        if self.poisoned.load(Ordering::Acquire) {
            Err(unknown())
        } else {
            Ok(())
        }
    }
    fn accept<T>(&self, result: QueueResult<T>) -> QueueResult<T> {
        if result.is_err() {
            self.poisoned.store(true, Ordering::Release);
        } else if self.poisoned.load(Ordering::Acquire) {
            return Err(unknown());
        }
        result
    }
    fn require(&self, valid: bool) -> QueueResult<()> {
        self.check()?;
        self.accept(if valid { Ok(()) } else { Err(unknown()) })
    }
    fn admit_key(&mut self, bytes: usize) -> QueueResult<()> {
        self.check()?;
        let result = (|| {
            let needed = (bytes as u64)
                .checked_mul(64)
                .and_then(|n| n.checked_add(65536))
                .ok_or(QueueError::TooLarge)?;
            if needed > self.key_peak {
                let next = needed
                    .checked_next_power_of_two()
                    .ok_or(QueueError::TooLarge)?;
                let charge = self._memory.child_buffer(next - self.key_peak)?;
                self.key_charges.push(charge);
                self.key_peak = next;
            }
            Ok(())
        })();
        self.accept(result)
    }
    async fn execute(&self, sql: &str, params: Vec<Value>) -> QueueResult<()> {
        self.check()?;
        let result = async {
            if !self.connection.is_autocommit().map_err(|_| unknown())? {
                return Err(unknown());
            }
            self.connection
                .execute(sql, params_from_iter(params))
                .await
                .map_err(|_| unknown())?;
            if !self.connection.is_autocommit().map_err(|_| unknown())? {
                return Err(unknown());
            }
            // Bound temporary WAL growth rather than retaining every completed
            // statement's frames until operation end. This owns scratch only.
            if self.statements.fetch_add(1, Ordering::Relaxed) % 64 == 63 {
                let checkpoint = self.one("PRAGMA wal_checkpoint(TRUNCATE)", vec![]).await?;
                if !matches!(
                    checkpoint.as_deref(),
                    Some([Value::Integer(0), Value::Integer(_), Value::Integer(_)])
                ) {
                    return Err(unknown());
                }
            }
            Ok(())
        }
        .await;
        self.accept(result)
    }
    async fn one(&self, sql: &str, params: Vec<Value>) -> QueueResult<Option<Vec<Value>>> {
        self.check()?;
        let result = async {
            if !self.connection.is_autocommit().map_err(|_| unknown())? {
                return Err(unknown());
            }
            let mut rows = self
                .connection
                .query(sql, params_from_iter(params))
                .await
                .map_err(|_| unknown())?;
            let Some(row) = rows.next().await.map_err(|_| unknown())? else {
                return Ok(None);
            };
            let values = crate::row_values(&row, row.column_count())
                .map_err(|_| unknown())?
                .collect();
            if rows.next().await.map_err(|_| unknown())?.is_some() {
                return Err(unknown());
            }
            drop(rows);
            if !self.connection.is_autocommit().map_err(|_| unknown())? {
                return Err(unknown());
            }
            Ok(Some(values))
        }
        .await;
        self.accept(result)
    }
    pub async fn record(&mut self, id: i64, key: &[u8], version: u64) -> QueueResult<()> {
        self.require(id > 0 && !key.is_empty() && version > 0)?;
        self.admit_key(key.len())?;
        self.execute(
            INSERT_RECORD,
            vec![
                Value::Integer(id),
                Value::Blob(key.to_vec()),
                Value::Blob(version.to_be_bytes().to_vec()),
            ],
        )
        .await?;
        let bound = self
            .one(
                CHECK_RECORD,
                vec![
                    Value::Integer(id),
                    Value::Blob(key.to_vec()),
                    Value::Blob(version.to_be_bytes().to_vec()),
                ],
            )
            .await?;
        self.require(bound.as_deref() == Some(&[Value::Integer(1)]))
    }
    pub async fn leaf(&self, node: i64, record: i64) -> QueueResult<()> {
        self.require(node >= 0 && record > 0)?;
        self.execute(
            INSERT_SET,
            vec![Value::Integer(node), Value::Integer(record)],
        )
        .await
    }
    pub async fn edge(&self, parent: i64, child: i64) -> QueueResult<()> {
        self.require(parent >= 0 && child >= 0)?;
        self.execute(
            INSERT_EDGE,
            vec![Value::Integer(parent), Value::Integer(child)],
        )
        .await
    }
    pub async fn union_child(&self, node: i64, record: i64, child: i64) -> QueueResult<()> {
        self.require(node >= 0 && record > 0 && child >= 0)?;
        self.execute(
            UNION,
            vec![
                Value::Integer(node),
                Value::Integer(record),
                Value::Integer(child),
            ],
        )
        .await
    }
    pub async fn intersection(&self, node: i64, record: i64) -> QueueResult<()> {
        self.require(node >= 0 && record > 0)?;
        self.execute(
            INTERSECTION,
            vec![Value::Integer(node), Value::Integer(record)],
        )
        .await
    }
    pub async fn complement(&self, node: i64, record: i64, child: i64) -> QueueResult<()> {
        self.require(node >= 0 && record > 0 && child >= 0)?;
        self.execute(
            COMPLEMENT,
            vec![
                Value::Integer(node),
                Value::Integer(record),
                Value::Integer(child),
            ],
        )
        .await
    }
    pub async fn owner(&self, record: i64, clause: i64) -> QueueResult<()> {
        self.require(record > 0 && clause >= 0)?;
        let exists = self.one(FIND_RECORD, vec![Value::Integer(record)]).await?;
        self.require(exists.as_deref() == Some(&[Value::Integer(1)]))?;
        if let Some(row) = self.one(FIND_OWNER, vec![Value::Integer(record)]).await? {
            self.require(matches!(row.as_slice(), [Value::Integer(first)] if *first <= clause))?;
        }
        self.execute(
            INSERT_OWNER,
            vec![Value::Integer(record), Value::Integer(clause)],
        )
        .await
    }
    /// Copy a completed native accumulator through a one-row cursor. The
    /// destination never reads itself, avoiding stock SDK ephemeral row sets.
    pub async fn finish_node(&self, node: i64) -> QueueResult<()> {
        self.require(node >= 0)?;
        let mut after = 0;
        loop {
            match self
                .one(
                    NEXT_ACCUMULATOR,
                    vec![Value::Integer(node), Value::Integer(after)],
                )
                .await?
                .as_deref()
            {
                None => return Ok(()),
                Some([Value::Integer(id)]) if *id > after => {
                    self.leaf(node, *id).await?;
                    after = *id;
                }
                _ => return self.accept(Err(unknown())),
            }
        }
    }
    pub async fn next_set(&self, node: i64, after: i64) -> QueueResult<Option<i64>> {
        self.require(node >= 0 && after >= 0)?;
        match self
            .one(NEXT_SET, vec![Value::Integer(node), Value::Integer(after)])
            .await?
            .as_deref()
        {
            None => Ok(None),
            Some([Value::Integer(id)]) if *id > after => Ok(Some(*id)),
            _ => self.accept(Err(unknown())),
        }
    }
    pub async fn next_owner(&mut self, after: &[u8]) -> QueueResult<Option<SelectedScratchRow>> {
        let result = self.read_owner(after).await;
        self.accept(result)
    }
    async fn read_owner(&mut self, after: &[u8]) -> QueueResult<Option<SelectedScratchRow>> {
        self.admit_key(after.len())?;
        let metadata = self
            .one(NEXT_OWNER, vec![Value::Blob(after.to_vec())])
            .await?;
        let Some(
            [
                Value::Integer(id),
                Value::Integer(clause),
                Value::Integer(bytes),
                Value::Blob(version),
            ],
        ) = metadata.as_deref()
        else {
            return if metadata.is_none() {
                Ok(None)
            } else {
                Err(unknown())
            };
        };
        let version = u64::from_be_bytes(version.as_slice().try_into().map_err(|_| unknown())?);
        self.require(*id > 0 && *clause >= 0 && *bytes > 0 && version > 0)?;
        self.admit_key(usize::try_from(*bytes).map_err(|_| unknown())?)?;
        let charge = self._memory.child_buffer(
            u64::try_from(*bytes)
                .map_err(|_| unknown())?
                .checked_mul(8)
                .and_then(|n| n.checked_add(4096))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let key = self.one(READ_OWNER_KEY, vec![Value::Integer(*id)]).await?;
        match key {
            Some(mut row) if row.len() == 1 => match row.remove(0) {
                Value::Blob(key) if key.len() as i64 == *bytes && key.as_slice() > after => {
                    Ok(Some(SelectedScratchRow {
                        id: *id,
                        clause: *clause,
                        version,
                        key,
                        _charge: charge,
                    }))
                }
                _ => Err(unknown()),
            },
            _ => Err(unknown()),
        }
    }
}
#[cfg(test)]
mod tests;
