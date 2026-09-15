//! object-log CLI: produce/consume opaque batches and inspect stores.
//!
//! This is intentionally a slightly odd shell citizen — the library is an
//! embeddable engine, not a stream processor — but treating files/stdin as
//! batch payloads makes black-box testing and visibility easy:
//!
//! ```text
//! # Round-trip line records through a local store
//! printf 'a\nb\nc\n' | object-log produce --root /tmp/olog --partition demo --lines
//! object-log consume --root /tmp/olog --partition demo --lines
//!
//! # Binary-safe framing (u64 BE length + payload)
//! object-log produce --root /tmp/olog --partition demo --framed file1.bin file2.bin
//! object-log consume --root /tmp/olog --partition demo --framed > out.framed
//!
//! cargo run --features cli --bin object-log -- --help
//! ```

use bytes::Bytes;
use clap::{Parser, Subcommand, ValueEnum};
use object_log::{
    BlobStore, Durability, FlushConfig, LocalBlobStore, LogEngine, ManifestSequencer, PartitionKey,
};
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
    name = "object-log",
    about = "Produce/consume opaque log batches and inspect object-log stores",
    long_about = "A small, slightly weird CLI over the object-log engine: inputs \
are files or stdin, outputs are stdout or files, and each unit is one opaque \
batch. Useful for black-box tests and operator visibility — not a Kafka client.",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Append batches from files and/or stdin.
    Produce {
        #[command(flatten)]
        store: StoreArgs,
        #[command(flatten)]
        engine: EngineArgs,
        /// Input files; use `-` for stdin. With no paths, reads stdin.
        #[arg(value_name = "PATH")]
        inputs: Vec<PathBuf>,
        /// How to split inputs into batches (default: whole file / whole stdin).
        #[arg(long, value_enum, default_value_t = IoMode::File)]
        mode: IoMode,
        /// Alias for `--mode lines`.
        #[arg(long, conflicts_with = "mode")]
        lines: bool,
        /// Alias for `--mode nul`.
        #[arg(long, conflicts_with_all = ["mode", "lines"])]
        nul: bool,
        /// Alias for `--mode framed`.
        #[arg(long, conflicts_with_all = ["mode", "lines", "nul"])]
        framed: bool,
        /// `record_count` stamped on each produce (default 1).
        #[arg(long, default_value_t = 1)]
        record_count: i32,
        /// Print JSON ack lines to stdout (`offset`, `bytes`, …). Status stays on stderr.
        #[arg(long)]
        acks: bool,
    },
    /// Read batches from a partition to stdout (or `--out-dir`).
    Consume {
        #[command(flatten)]
        store: StoreArgs,
        #[command(flatten)]
        engine: EngineArgs,
        /// Inclusive start offset.
        #[arg(long, default_value_t = 0)]
        from: i64,
        /// Stop after this many batches (default: all remaining in the first fetch window).
        #[arg(long)]
        limit: Option<usize>,
        /// Byte budget per internal fetch call.
        #[arg(long, default_value_t = 4 << 20)]
        max_bytes: usize,
        /// Output framing (default: raw concatenation of payloads).
        #[arg(long, value_enum, default_value_t = IoMode::Raw)]
        mode: IoMode,
        #[arg(long, conflicts_with = "mode")]
        lines: bool,
        #[arg(long, conflicts_with_all = ["mode", "lines"])]
        nul: bool,
        #[arg(long, conflicts_with_all = ["mode", "lines", "nul"])]
        framed: bool,
        /// Write each batch to `OUT_DIR/<offset>.bin` instead of stdout.
        #[arg(long)]
        out_dir: Option<PathBuf>,
        /// Print JSON metadata lines to stderr (offset, bytes, record_count).
        #[arg(long)]
        meta: bool,
    },
    /// One-shot black-box: produce inputs then consume from offset 0 to stdout.
    ///
    /// With `--memory`, the store lives only for this process (no --root needed).
    Roundtrip {
        #[command(flatten)]
        store: StoreArgs,
        #[command(flatten)]
        engine: EngineArgs,
        #[arg(value_name = "PATH")]
        inputs: Vec<PathBuf>,
        #[arg(long, value_enum, default_value_t = IoMode::File)]
        mode: IoMode,
        #[arg(long, conflicts_with = "mode")]
        lines: bool,
        #[arg(long, conflicts_with_all = ["mode", "lines"])]
        nul: bool,
        #[arg(long, conflicts_with_all = ["mode", "lines", "nul"])]
        framed: bool,
        #[arg(long, default_value_t = 1)]
        record_count: i32,
        /// Use in-process MemoryBlobStore (ignores --root / S3).
        #[arg(long)]
        memory: bool,
    },
    /// List object keys under a prefix.
    List {
        #[command(flatten)]
        store: StoreArgs,
        #[arg(long, default_value = "")]
        prefix: String,
    },
    /// Print the rebuilt ManifestSequencer index.
    Inspect {
        #[command(flatten)]
        store: StoreArgs,
        #[arg(long, default_value = "_manifest/")]
        manifest_prefix: String,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        summary: bool,
    },
    /// Find (and optionally delete) data objects not referenced by the index.
    Orphans {
        #[command(flatten)]
        store: StoreArgs,
        #[arg(long, default_value = "log/")]
        data_prefix: String,
        #[arg(long, default_value = "_manifest/")]
        manifest_prefix: String,
        #[arg(long)]
        delete: bool,
    },
    /// Low-level fetch with hex/text preview (prefer `consume` for bulk export).
    Fetch {
        #[command(flatten)]
        store: StoreArgs,
        #[command(flatten)]
        engine: EngineArgs,
        #[arg(long, default_value_t = 0)]
        offset: i64,
        #[arg(long, default_value_t = 1 << 20)]
        max_bytes: usize,
        #[arg(long)]
        text: bool,
    },
}

/// How batches are split on input / joined on output.
#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum IoMode {
    /// Each path is one batch; bare stdin is one batch.
    File,
    /// Newline-delimited records (trailing newline optional).
    Lines,
    /// NUL-delimited records.
    Nul,
    /// u64 big-endian length + payload, repeated (binary-safe).
    Framed,
    /// Consume only: concatenate payloads with no separator.
    Raw,
}

#[derive(clap::Args, Debug)]
struct StoreArgs {
    /// Local directory root for LocalBlobStore.
    #[arg(long, global = true, conflicts_with_all = ["s3_endpoint", "s3_bucket"])]
    root: Option<PathBuf>,
    #[arg(long, global = true)]
    s3_endpoint: Option<String>,
    #[arg(long, global = true)]
    s3_bucket: Option<String>,
    #[arg(long, global = true, default_value = "us-east-1")]
    s3_region: String,
    #[arg(long, global = true, env = "OBJECT_LOG_S3_KEY_ID")]
    s3_key_id: Option<String>,
    #[arg(long, global = true, env = "OBJECT_LOG_S3_SECRET")]
    s3_secret: Option<String>,
}

#[derive(clap::Args, Debug, Clone)]
struct EngineArgs {
    #[arg(long, global = true, default_value = "log/")]
    data_prefix: String,
    #[arg(long, global = true, default_value = "_manifest/")]
    manifest_prefix: String,
    /// Partition key (required for produce/consume/fetch/roundtrip).
    #[arg(long, global = true, default_value = "default")]
    partition: String,
    /// Durability for produce (default: sequenced).
    #[arg(long, global = true, value_enum, default_value_t = DurArg::Sequenced)]
    durability: DurArg,
    /// Linger in milliseconds (0 = seal ASAP). Default 50.
    #[arg(long, global = true, default_value_t = 50)]
    linger_ms: u64,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum DurArg {
    Buffered,
    Durable,
    Sequenced,
}

impl From<DurArg> for Durability {
    fn from(v: DurArg) -> Self {
        match v {
            DurArg::Buffered => Durability::Buffered,
            DurArg::Durable => Durability::Durable,
            DurArg::Sequenced => Durability::Sequenced,
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let cli = Cli::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    rt.block_on(async move {
        match cli.command {
            Command::Produce {
                store,
                engine,
                inputs,
                mode,
                lines,
                nul,
                framed,
                record_count,
                acks,
            } => {
                let mode = resolve_mode(mode, lines, nul, framed, IoMode::File)?;
                cmd_produce(store, engine, inputs, mode, record_count, acks).await
            }
            Command::Consume {
                store,
                engine,
                from,
                limit,
                max_bytes,
                mode,
                lines,
                nul,
                framed,
                out_dir,
                meta,
            } => {
                let mode = resolve_mode(mode, lines, nul, framed, IoMode::Raw)?;
                cmd_consume(store, engine, from, limit, max_bytes, mode, out_dir, meta).await
            }
            Command::Roundtrip {
                store,
                engine,
                inputs,
                mode,
                lines,
                nul,
                framed,
                record_count,
                memory,
            } => {
                let mode = resolve_mode(mode, lines, nul, framed, IoMode::File)?;
                cmd_roundtrip(store, engine, inputs, mode, record_count, memory).await
            }
            Command::List { store, prefix } => cmd_list(store, prefix).await,
            Command::Inspect {
                store,
                manifest_prefix,
                json,
                summary,
            } => cmd_inspect(store, manifest_prefix, json, summary).await,
            Command::Orphans {
                store,
                data_prefix,
                manifest_prefix,
                delete,
            } => cmd_orphans(store, data_prefix, manifest_prefix, delete).await,
            Command::Fetch {
                store,
                engine,
                offset,
                max_bytes,
                text,
            } => cmd_fetch(store, engine, offset, max_bytes, text).await,
        }
    })
}

fn resolve_mode(
    mode: IoMode,
    lines: bool,
    nul: bool,
    framed: bool,
    _default: IoMode,
) -> Result<IoMode, String> {
    let flags = [lines, nul, framed].into_iter().filter(|x| *x).count();
    if flags > 1 {
        return Err("use only one of --lines / --nul / --framed".into());
    }
    if lines {
        return Ok(IoMode::Lines);
    }
    if nul {
        return Ok(IoMode::Nul);
    }
    if framed {
        return Ok(IoMode::Framed);
    }
    Ok(mode)
}

// ---- store / engine helpers -------------------------------------------------

async fn open_store(args: &StoreArgs) -> Result<Arc<dyn BlobStore>, String> {
    if let Some(root) = &args.root {
        fs::create_dir_all(root).map_err(|e| format!("create root: {e}"))?;
        return Ok(Arc::new(LocalBlobStore::new(root)));
    }

    #[cfg(feature = "s3")]
    {
        let endpoint = args
            .s3_endpoint
            .as_deref()
            .ok_or("provide --root DIR or --s3-endpoint (or --memory on roundtrip)")?;
        let bucket = args
            .s3_bucket
            .as_deref()
            .ok_or("--s3-bucket is required with --s3-endpoint")?;
        let key_id = args
            .s3_key_id
            .as_deref()
            .ok_or("--s3-key-id or OBJECT_LOG_S3_KEY_ID is required")?;
        let secret = args
            .s3_secret
            .as_deref()
            .ok_or("--s3-secret or OBJECT_LOG_S3_SECRET is required")?;
        Ok(Arc::new(object_log::S3BlobStore::new(
            endpoint,
            &args.s3_region,
            bucket,
            key_id,
            secret,
        )))
    }

    #[cfg(not(feature = "s3"))]
    {
        let _ = (
            &args.s3_endpoint,
            &args.s3_bucket,
            &args.s3_key_id,
            &args.s3_secret,
            &args.s3_region,
        );
        Err("provide --root DIR (or rebuild with --features cli,s3 for S3)".into())
    }
}

fn flush_config(engine: &EngineArgs) -> FlushConfig {
    FlushConfig {
        linger: Duration::from_millis(engine.linger_ms),
        budget: object_log::BudgetConfig {
            enabled: false,
            ..Default::default()
        },
        ..FlushConfig::default()
    }
}

async fn open_engine(
    store: &StoreArgs,
    engine: &EngineArgs,
) -> Result<LogEngine<ManifestSequencer>, String> {
    let blob = open_store(store).await?;
    let seq = Arc::new(
        ManifestSequencer::open(Arc::clone(&blob), engine.manifest_prefix.clone())
            .await
            .map_err(|e| e.to_string())?,
    );
    Ok(LogEngine::new(
        blob,
        seq,
        flush_config(engine),
        engine.data_prefix.clone(),
    ))
}

// ---- produce / consume framing ---------------------------------------------

fn collect_batches(inputs: &[PathBuf], mode: IoMode) -> Result<Vec<Bytes>, String> {
    let paths: Vec<PathBuf> = if inputs.is_empty() {
        vec![PathBuf::from("-")]
    } else {
        inputs.to_vec()
    };

    match mode {
        IoMode::File => {
            let mut out = Vec::new();
            for p in paths {
                out.push(Bytes::from(read_path(&p)?));
            }
            Ok(out)
        }
        IoMode::Lines | IoMode::Nul | IoMode::Framed => {
            let mut buf = Vec::new();
            for p in paths {
                buf.extend(read_path(&p)?);
            }
            split_buffer(&buf, mode)
        }
        IoMode::Raw => {
            Err("--mode raw is consume-only; use file/lines/nul/framed for produce".into())
        }
    }
}

fn read_path(p: &Path) -> Result<Vec<u8>, String> {
    if p.as_os_str() == "-" {
        let mut v = Vec::new();
        io::stdin()
            .read_to_end(&mut v)
            .map_err(|e| format!("read stdin: {e}"))?;
        Ok(v)
    } else {
        fs::read(p).map_err(|e| format!("read {}: {e}", p.display()))
    }
}

fn split_buffer(buf: &[u8], mode: IoMode) -> Result<Vec<Bytes>, String> {
    match mode {
        IoMode::Lines => {
            if buf.is_empty() {
                return Ok(vec![]);
            }
            let mut out = Vec::new();
            let mut start = 0usize;
            for (i, b) in buf.iter().enumerate() {
                if *b == b'\n' {
                    out.push(Bytes::copy_from_slice(&buf[start..i]));
                    start = i + 1;
                }
            }
            if start < buf.len() {
                out.push(Bytes::copy_from_slice(&buf[start..]));
            } else if buf.ends_with(b"\n") {
                // trailing newline already closed last record; ok
            }
            Ok(out)
        }
        IoMode::Nul => {
            if buf.is_empty() {
                return Ok(vec![]);
            }
            let mut out = Vec::new();
            let mut start = 0usize;
            for (i, b) in buf.iter().enumerate() {
                if *b == 0 {
                    out.push(Bytes::copy_from_slice(&buf[start..i]));
                    start = i + 1;
                }
            }
            if start < buf.len() {
                out.push(Bytes::copy_from_slice(&buf[start..]));
            }
            Ok(out)
        }
        IoMode::Framed => {
            let mut out = Vec::new();
            let mut i = 0usize;
            while i < buf.len() {
                if buf.len() - i < 8 {
                    return Err(format!(
                        "truncated framed stream at byte {i} (need 8-byte length)"
                    ));
                }
                let len = u64::from_be_bytes(buf[i..i + 8].try_into().unwrap()) as usize;
                i += 8;
                if buf.len() - i < len {
                    return Err(format!(
                        "truncated framed payload at byte {i} (need {len} bytes)"
                    ));
                }
                out.push(Bytes::copy_from_slice(&buf[i..i + len]));
                i += len;
            }
            Ok(out)
        }
        IoMode::File | IoMode::Raw => unreachable!("split_buffer only for stream modes"),
    }
}

fn write_batch(out: &mut dyn Write, payload: &[u8], mode: IoMode) -> Result<(), String> {
    match mode {
        IoMode::Raw | IoMode::File => {
            out.write_all(payload).map_err(|e| e.to_string())?;
        }
        IoMode::Lines => {
            out.write_all(payload).map_err(|e| e.to_string())?;
            out.write_all(b"\n").map_err(|e| e.to_string())?;
        }
        IoMode::Nul => {
            out.write_all(payload).map_err(|e| e.to_string())?;
            out.write_all(&[0]).map_err(|e| e.to_string())?;
        }
        IoMode::Framed => {
            let len = (payload.len() as u64).to_be_bytes();
            out.write_all(&len).map_err(|e| e.to_string())?;
            out.write_all(payload).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

// ---- commands --------------------------------------------------------------

async fn cmd_produce(
    store: StoreArgs,
    engine: EngineArgs,
    inputs: Vec<PathBuf>,
    mode: IoMode,
    record_count: i32,
    acks: bool,
) -> Result<(), String> {
    if record_count <= 0 {
        return Err("--record-count must be > 0".into());
    }
    let batches = collect_batches(&inputs, mode)?;
    if batches.is_empty() {
        eprintln!("# nothing to produce");
        return Ok(());
    }
    let eng = open_engine(&store, &engine).await?;
    let p = PartitionKey(engine.partition.clone());
    let dur: Durability = engine.durability.into();
    let mut n = 0usize;
    for payload in batches {
        let bytes = payload.len();
        let out = eng
            .produce(p.clone(), payload, record_count, (), dur)
            .await
            .map_err(|e| e.to_string())?;
        n += 1;
        if acks {
            println!(
                "{{\"base_offset\":{},\"last_offset\":{},\"bytes\":{},\"durable\":{},\"sequenced\":{}}}",
                out.base_offset
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "null".into()),
                out.last_offset
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "null".into()),
                bytes,
                out.durable,
                out.sequenced
            );
        }
    }
    if matches!(dur, Durability::Buffered) {
        eng.flush().await.map_err(|e| e.to_string())?;
    }
    eprintln!("# produced {n} batch(es) partition={:?}", engine.partition);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn cmd_consume(
    store: StoreArgs,
    engine: EngineArgs,
    from: i64,
    limit: Option<usize>,
    max_bytes: usize,
    mode: IoMode,
    out_dir: Option<PathBuf>,
    meta: bool,
) -> Result<(), String> {
    let eng = open_engine(&store, &engine).await?;
    let p = PartitionKey(engine.partition.clone());
    let mut offset = from;
    let mut emitted = 0usize;
    let mut stdout = io::stdout().lock();

    if let Some(dir) = &out_dir {
        fs::create_dir_all(dir).map_err(|e| format!("out-dir: {e}"))?;
    }

    loop {
        if limit.is_some_and(|l| emitted >= l) {
            break;
        }
        let batches = eng
            .fetch(&p, offset, max_bytes)
            .await
            .map_err(|e| e.to_string())?;
        if batches.is_empty() {
            break;
        }
        for b in batches {
            if limit.is_some_and(|l| emitted >= l) {
                break;
            }
            if let Some(dir) = &out_dir {
                let path = dir.join(format!("{}.bin", b.base_offset));
                fs::write(&path, &b.payload)
                    .map_err(|e| format!("write {}: {e}", path.display()))?;
            } else {
                write_batch(&mut stdout, &b.payload, mode)?;
            }
            if meta {
                eprintln!(
                    "{{\"base_offset\":{},\"record_count\":{},\"bytes\":{}}}",
                    b.base_offset,
                    b.record_count,
                    b.payload.len()
                );
            }
            offset = b.base_offset + b.record_count as i64;
            emitted += 1;
        }
    }
    stdout.flush().map_err(|e| e.to_string())?;
    eprintln!("# consumed {emitted} batch(es) next_offset={offset}");
    Ok(())
}

async fn cmd_roundtrip(
    store: StoreArgs,
    engine: EngineArgs,
    inputs: Vec<PathBuf>,
    mode: IoMode,
    record_count: i32,
    memory: bool,
) -> Result<(), String> {
    if record_count <= 0 {
        return Err("--record-count must be > 0".into());
    }
    let batches = collect_batches(&inputs, mode)?;
    if batches.is_empty() {
        return Err("no input batches".into());
    }

    let (blob, data_prefix, manifest_prefix): (Arc<dyn BlobStore>, String, String) = if memory {
        (
            Arc::new(object_log::MemoryBlobStore::new()),
            engine.data_prefix.clone(),
            engine.manifest_prefix.clone(),
        )
    } else {
        (
            open_store(&store).await?,
            engine.data_prefix.clone(),
            engine.manifest_prefix.clone(),
        )
    };

    let seq = Arc::new(
        ManifestSequencer::open(Arc::clone(&blob), manifest_prefix)
            .await
            .map_err(|e| e.to_string())?,
    );
    let eng = LogEngine::new(blob, seq, flush_config(&engine), data_prefix);
    let p = PartitionKey(engine.partition.clone());
    let dur: Durability = engine.durability.into();

    for payload in &batches {
        eng.produce(p.clone(), payload.clone(), record_count, (), dur)
            .await
            .map_err(|e| e.to_string())?;
    }
    if matches!(dur, Durability::Buffered) {
        eng.flush().await.map_err(|e| e.to_string())?;
    }

    // Consume with the same framing as produce (file→raw-ish: framed modes match).
    let out_mode = match mode {
        IoMode::File => IoMode::Framed, // preserve binary batch boundaries
        other => other,
    };
    let mut stdout = io::stdout().lock();
    let mut n = 0usize;
    eng.fetch_stream(&p, 0, |b| {
        write_batch(&mut stdout, &b.payload, out_mode)
            .map_err(object_log::ObjectLogError::InvalidBatch)?;
        n += 1;
        Ok(())
    })
    .await
    .map_err(|e| e.to_string())?;
    stdout.flush().map_err(|e| e.to_string())?;

    if n != batches.len() {
        return Err(format!(
            "roundtrip count mismatch: produced {} consumed {n}",
            batches.len()
        ));
    }
    eprintln!(
        "# roundtrip ok batches={n} partition={:?}",
        engine.partition
    );
    Ok(())
}

async fn cmd_list(store: StoreArgs, prefix: String) -> Result<(), String> {
    let blob = open_store(&store).await?;
    let mut keys = blob.list(&prefix).await.map_err(|e| e.to_string())?;
    keys.sort();
    for k in &keys {
        println!("{k}");
    }
    eprintln!("# {} key(s)", keys.len());
    Ok(())
}

async fn cmd_inspect(
    store: StoreArgs,
    manifest_prefix: String,
    json: bool,
    summary: bool,
) -> Result<(), String> {
    let blob = open_store(&store).await?;
    let seq = ManifestSequencer::open(blob, manifest_prefix)
        .await
        .map_err(|e| e.to_string())?;
    let mut snap = seq.snapshot();
    if summary {
        for p in &mut snap.partitions {
            p.entries.clear();
        }
    }
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&snap).map_err(|e| e.to_string())?
        );
        return Ok(());
    }
    println!(
        "manifest_prefix={}  manifests={}  partitions={}",
        snap.manifest_prefix,
        snap.manifest_count,
        snap.partitions.len()
    );
    for p in &snap.partitions {
        println!(
            "  partition={:?}  log_start={}  hwm={}  entries={}",
            p.partition, p.log_start, p.high_watermark, p.entry_count
        );
        if !summary {
            for e in &p.entries {
                println!(
                    "    base={} count={} object={} range={}+{}",
                    e.base_offset,
                    e.record_count,
                    e.location.object_id,
                    e.location.byte_start,
                    e.location.byte_len
                );
            }
        }
    }
    Ok(())
}

async fn cmd_orphans(
    store: StoreArgs,
    data_prefix: String,
    manifest_prefix: String,
    delete: bool,
) -> Result<(), String> {
    let blob = open_store(&store).await?;
    let seq = ManifestSequencer::open(Arc::clone(&blob), manifest_prefix)
        .await
        .map_err(|e| e.to_string())?;
    let live = seq.live_object_ids();
    let mut keys = blob.list(&data_prefix).await.map_err(|e| e.to_string())?;
    keys.sort();
    let mut orphans = Vec::new();
    for k in keys {
        if !live.contains(&k) {
            orphans.push(k);
        }
    }
    if orphans.is_empty() {
        eprintln!("no orphans under {data_prefix:?} (live={})", live.len());
        return Ok(());
    }
    for k in &orphans {
        println!("{k}");
    }
    eprintln!(
        "# {} orphan(s); live_index_objects={}",
        orphans.len(),
        live.len()
    );
    if delete {
        for k in &orphans {
            blob.delete(k).await.map_err(|e| e.to_string())?;
        }
        eprintln!("# deleted {}", orphans.len());
    } else {
        eprintln!("# dry-run only; pass --delete to remove");
    }
    Ok(())
}

async fn cmd_fetch(
    store: StoreArgs,
    engine: EngineArgs,
    offset: i64,
    max_bytes: usize,
    text: bool,
) -> Result<(), String> {
    let eng = open_engine(&store, &engine).await?;
    let p = PartitionKey(engine.partition.clone());
    let batches = eng
        .fetch(&p, offset, max_bytes)
        .await
        .map_err(|e| e.to_string())?;
    if batches.is_empty() {
        eprintln!("# no batches at/after offset {offset}");
        return Ok(());
    }
    for b in &batches {
        if text {
            println!(
                "offset={} count={} payload={}",
                b.base_offset,
                b.record_count,
                String::from_utf8_lossy(&b.payload)
            );
        } else {
            let preview: String = b
                .payload
                .iter()
                .take(16)
                .map(|x| format!("{x:02x}"))
                .collect::<Vec<_>>()
                .join("");
            let more = if b.payload.len() > 16 { "…" } else { "" };
            println!(
                "offset={} count={} bytes={} hex={preview}{more}",
                b.base_offset,
                b.record_count,
                b.payload.len()
            );
        }
    }
    eprintln!("# {} batch(es)", batches.len());
    Ok(())
}
