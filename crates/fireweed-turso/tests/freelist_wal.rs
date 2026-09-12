#![cfg(feature = "local")]
use std::sync::{Arc, Mutex};
use turso_core::io::{FileId, FileSyncType};
use turso_core::{
    Buffer, Clock, Completion, Connection, Database, File, IO, MemoryIO, MonotonicInstant,
    OpenFlags, StepResult, WallClockInstant,
};

type Result<T> = turso_core::Result<T>;
#[derive(Default, Clone, Copy, Debug)]
struct Writes {
    bytes: usize,
    nonzero: usize,
}
struct ObservedIo {
    inner: MemoryIO,
    writes: Arc<Mutex<Writes>>,
}
impl Clock for ObservedIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}
impl IO for ObservedIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        if path.ends_with("-wal") {
            Ok(Arc::new(ObservedWal {
                inner,
                writes: self.writes.clone(),
            }))
        } else {
            Ok(inner)
        }
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }
    fn file_id(&self, path: &str) -> Result<FileId> {
        self.inner.file_id(path)
    }
}
struct ObservedWal {
    inner: Arc<dyn File>,
    writes: Arc<Mutex<Writes>>,
}
impl ObservedWal {
    fn observe(&self, buffer: &Buffer) {
        let bytes = buffer.as_slice();
        let mut writes = self.writes.lock().unwrap();
        writes.bytes += bytes.len();
        writes.nonzero += bytes.iter().filter(|&&b| b != 0).count();
    }
}
impl File for ObservedWal {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        self.inner.pread(pos, c)
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.observe(&buffer);
        self.inner.pwrite(pos, buffer, c)
    }
    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        for buffer in &buffers {
            self.observe(buffer);
        }
        self.inner.pwritev(pos, buffers, c)
    }
    fn sync(&self, c: Completion, mode: FileSyncType) -> Result<Completion> {
        self.inner.sync(c, mode)
    }
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.inner.truncate(len, c)
    }
}
fn body(id: usize, bytes: usize) -> String {
    let mut state = id as u64 + 17;
    (0..bytes)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (b'a' + (state % 26) as u8) as char
        })
        .collect()
}
fn insert(conn: &Arc<Connection>, first: usize, count: usize, bytes: usize) {
    for id in first..first + count {
        conn.execute(format!(
            "INSERT INTO items VALUES({id},'{}')",
            body(id, bytes)
        ))
        .unwrap();
    }
}
fn verify(conn: &Arc<Connection>, first: usize, count: usize, bytes: usize) {
    let mut query = conn
        .prepare("SELECT id,body FROM items ORDER BY id")
        .unwrap();
    let mut seen = 0;
    loop {
        match query.step().unwrap() {
            StepResult::Row => {
                let row = query.row().unwrap();
                assert_eq!(row.get::<i64>(0).unwrap(), (first + seen) as i64);
                assert_eq!(row.get::<String>(1).unwrap(), body(first + seen, bytes));
                seen += 1;
            }
            StepResult::Done => break,
            other => panic!("unexpected memory query step {other:?}"),
        }
    }
    assert_eq!(seen, count);
}
#[test]
fn free_page_history_preserves_readers_rollback_and_reuse() {
    for cache_kib in [32768, 64] {
        for bytes in [900, 5000] {
            exercise(cache_kib, bytes);
        }
    }
}
fn exercise(cache_kib: usize, bytes: usize) {
    const COUNT: usize = 1024;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("freelist.db");
    let path = path.to_str().unwrap();
    let writes = Arc::new(Mutex::new(Writes::default()));
    let io: Arc<dyn IO> = Arc::new(ObservedIo {
        inner: MemoryIO::new(),
        writes: writes.clone(),
    });
    let db = Database::open_file(io.clone(), path).unwrap();
    let writer = db.connect().unwrap();
    writer.execute("PRAGMA wal_autocheckpoint=0").unwrap();
    writer
        .execute(format!("PRAGMA cache_size=-{cache_kib}"))
        .unwrap();
    writer
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY,body TEXT)")
        .unwrap();
    writer.execute("BEGIN").unwrap();
    insert(&writer, 1, COUNT, bytes);
    writer.execute("COMMIT").unwrap();
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    // Undo a purge and page reuse, including cache-spill and overflow cases.
    writer.execute("BEGIN").unwrap();
    writer.execute("SAVEPOINT retained").unwrap();
    writer.execute("DELETE FROM items WHERE id<=512").unwrap();
    insert(&writer, 2000, 512, bytes);
    writer.execute("ROLLBACK TO retained").unwrap();
    writer.execute("RELEASE retained").unwrap();
    writer.execute("COMMIT").unwrap();
    verify(&writer, 1, COUNT, bytes);
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let reader = db.connect().unwrap();
    reader.execute("BEGIN").unwrap();
    verify(&reader, 1, COUNT, bytes);
    *writes.lock().unwrap() = Writes::default();
    // An addressed purge, not the special whole-table Clear opcode.
    writer.execute("DELETE FROM items WHERE id<=1024").unwrap();
    let observed = *writes.lock().unwrap();
    eprintln!("freelist WAL cache={cache_kib}KiB body={bytes}: {observed:?}");
    verify(&writer, 1, 0, bytes);
    verify(&reader, 1, COUNT, bytes);
    reader.execute("ROLLBACK").unwrap();
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(reader);
    drop(writer);
    drop(db);
    let db = Database::open_file(io, path).unwrap();
    let conn = db.connect().unwrap();
    verify(&conn, 1, 0, bytes);
    conn.execute("BEGIN").unwrap();
    insert(&conn, 5000, COUNT, bytes);
    conn.execute("COMMIT").unwrap();
    verify(&conn, 5000, COUNT, bytes);
    assert!(
        observed.bytes > 128 * 1024,
        "exercise many dirty freed pages"
    );
    assert!(
        observed.bytes < 2 * 1024 * 1024,
        "clean overflow leaves must not be dirtied just to clear them"
    );
}
