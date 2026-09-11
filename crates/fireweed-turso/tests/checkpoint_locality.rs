#![cfg(feature = "local")]
use std::sync::{Arc, Mutex};
use turso_core::io::FileSyncType;
use turso_core::storage::database::{DatabaseFile, DatabaseStorage};
use turso_core::{Buffer, Completion, Database, IO, MemoryIO, OpenFlags};

struct ObservedCheckpointStorage {
    inner: DatabaseFile,
    writes: Mutex<Vec<(usize, usize)>>,
}

impl DatabaseStorage for ObservedCheckpointStorage {
    fn read_header(&self, c: Completion) -> turso_core::Result<Completion> {
        self.inner.read_header(c)
    }
    fn read_page(
        &self,
        page: usize,
        ctx: &turso_core::storage::database::IOContext,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.inner.read_page(page, ctx, c)
    }
    fn write_page(
        &self,
        page: usize,
        buffer: Arc<Buffer>,
        ctx: &turso_core::storage::database::IOContext,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.writes.lock().unwrap().push((page, 1));
        self.inner.write_page(page, buffer, ctx, c)
    }
    fn write_pages(
        &self,
        page: usize,
        size: usize,
        buffers: Vec<Arc<Buffer>>,
        ctx: &turso_core::storage::database::IOContext,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.writes.lock().unwrap().push((page, buffers.len()));
        self.inner.write_pages(page, size, buffers, ctx, c)
    }
    fn sync(&self, c: Completion, mode: FileSyncType) -> turso_core::Result<Completion> {
        self.inner.sync(c, mode)
    }
    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: usize, c: Completion) -> turso_core::Result<Completion> {
        self.inner.truncate(len, c)
    }
}

#[test]
fn checkpoint_coalesces_reused_pages_and_preserves_latest_values() {
    for cache_kib in [32768, 512] {
        check_checkpoint_locality(cache_kib);
    }
}

fn check_checkpoint_locality(cache_kib: usize) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("locality.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let storage = Arc::new(ObservedCheckpointStorage {
        inner: DatabaseFile::new(io.open_file(path, OpenFlags::Create, false).unwrap()),
        writes: Mutex::new(Vec::new()),
    });
    let db = Database::open(io.clone(), path, storage.clone()).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA wal_autocheckpoint=0").unwrap();
    conn.execute("PRAGMA cache_size=-32768").unwrap();
    conn.execute("CREATE TABLE test(id INTEGER PRIMARY KEY,value TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for id in 0..2048 {
        conn.execute(format!(
            "INSERT INTO test VALUES({id},printf('%03000d',{id}))"
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("PRAGMA wal_checkpoint(FULL)").unwrap();
    // Spread each bounded WAL-frame batch across the existing destination
    // pages. All rows keep their original size and page residency.
    for n in 0..2048 {
        let id = n * 997 % 2048;
        conn.execute(format!(
            "UPDATE test SET value=printf('%03000d',id+2048) WHERE id={id}"
        ))
        .unwrap();
    }
    conn.execute(format!("PRAGMA cache_size=-{cache_kib}"))
        .unwrap();
    storage.writes.lock().unwrap().clear();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let writes = storage.writes.lock().unwrap().clone();
    let pages: usize = writes.iter().map(|(_, len)| len).sum();
    eprintln!(
        "checkpoint destination writes: {} calls for {pages} pages",
        writes.len()
    );
    assert!(
        pages > 512 * 2,
        "exercise multiple bounded batches: {pages}"
    );
    assert!(
        writes.len() * 16 < pages,
        "checkpoint should coalesce adjacent destination pages despite interleaved WAL frames: {} calls for {pages} pages",
        writes.len()
    );
    drop(conn);
    drop(db);
    // Truncation removed the WAL: this query must obtain the newest values
    // from the checkpointed database, not from the old pager cache or WAL.
    let reopened = Database::open(io, path, storage).unwrap();
    let conn = reopened.connect().unwrap();
    let mut statement = conn.prepare("SELECT count(*),sum(CAST(value AS INTEGER)),sum(CAST(value AS INTEGER)!=id+2048),min(id),max(id) FROM test").unwrap();
    let mut observed_rows = 0;
    loop {
        match statement.step().unwrap() {
            turso_core::StepResult::Row => {
                observed_rows += 1;
                let row = statement.row().unwrap();
                assert_eq!(row.get::<i64>(0).unwrap(), 2048);
                assert_eq!(row.get::<i64>(1).unwrap(), (2048..4096).sum::<i64>());
                assert_eq!(row.get::<i64>(2).unwrap(), 0);
                assert_eq!(row.get::<i64>(3).unwrap(), 0);
                assert_eq!(row.get::<i64>(4).unwrap(), 2047);
            }
            turso_core::StepResult::Done => break,
            other => panic!("unexpected memory query step: {other:?}"),
        }
    }
    assert_eq!(observed_rows, 1);
}
