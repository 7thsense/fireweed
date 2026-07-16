//! Release-evidence verification ledger (TP-001/TP-002/TP-003).
//!
//! A *verification ledger* is an append-only JSONL file under `target/pqueue-ledger/`. Each line is one
//! [`LedgerRow`] recording a measured release-evidence run: which suite/command produced it, the
//! backend profile + scale + seed + environment it ran under, the acceptance/invariant ids and TP-002
//! evidence ids it substantiates, the pass bar, the exit status, and the measured values. Evidence suites
//! append rows via [`append_row`]; the CI gate runs the `pqueue-verify-ledger` binary to strict-validate a
//! ledger and assert that required evidence ids (E0–E3) are present.
//!
//! This is the hexagonal-era rebuild of the ledger schema + verifier that lived in the removed
//! `pqueue-service` crate. The required fields match what `scripts/ci/release-gate.sh` validates.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};

pub mod attestation;

/// One verification-ledger row: a single measured release-evidence run.
///
/// Every field is required (a row that fails to deserialize is rejected by the verifier). `measurements`
/// carries the TP-002 evidence ids plus the measured values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerRow {
    /// The named test suite that produced this row (e.g. `performance_cross_queue_scale_out_tests`).
    pub suite: String,
    /// The exact command that produced it (for reproduction).
    pub command: String,
    /// Backend profile under test (`postgres_native` | `object_log_sqlite_projection` | `memory` | ...).
    pub backend_profile: String,
    /// Scale shape (`smoke` | `release` | a specific `S=…` descriptor).
    pub scale: String,
    /// Deterministic seed for the run (`0` = no seed / wall-clock-timed run with no seeded randomness).
    pub seed: u64,
    /// Where it ran (host class / CI lane / `in-process`).
    pub environment: String,
    /// Process exit status of the producing command. A non-zero status is NOT evidence (the run failed).
    pub exit_status: i32,
    /// Acceptance-criterion ids this row substantiates (e.g. `AC-E2E-1`). May be empty for pure scale rows.
    #[serde(default)]
    pub ac_ids: Vec<String>,
    /// Invariant ids held during the run (e.g. `INV-1`). May be empty.
    #[serde(default)]
    pub inv_ids: Vec<String>,
    /// The pass bar this row was judged against (human-readable).
    pub pass_bar: String,
    /// Evidence tier: `release` (counts toward the headline E0–E3 requirement) or `smoke` (an in-process or
    /// reduced-scale run — recorded and strict-validated for visibility, but NOT accepted as headline
    /// evidence by the gate). Absent → `release` (a row is release evidence unless it says otherwise; the
    /// new in-process suites set `smoke` explicitly). The gate's required-evidence assertion only counts
    /// `tp002_evidence_ids` from non-smoke rows.
    #[serde(default = "default_tier")]
    pub evidence_tier: String,
    /// Measured values + the TP-002 evidence ids substantiated.
    pub measurements: Measurements,
}

fn default_tier() -> String {
    "release".to_string()
}

/// Measured values for a row. [`tp002_evidence_ids`](Self::tp002_evidence_ids) names the E0–E3 records this
/// row substantiates; arbitrary additional measured key/values are kept in [`values`](Self::values).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Measurements {
    /// TP-002 evidence ids this row substantiates (`E0` | `E1` | `E2` | `E3`).
    #[serde(default)]
    pub tp002_evidence_ids: Vec<String>,
    /// Any additional measured values (throughput, latency percentiles, recovery time, …).
    #[serde(flatten, default)]
    pub values: BTreeMap<String, serde_json::Value>,
}

impl LedgerRow {
    /// Serialize this row to a single JSONL line (no trailing newline).
    pub fn to_jsonl(&self) -> String {
        serde_json::to_string(self).expect("LedgerRow serializes")
    }
}

/// The ledger file an evidence suite writes its row to: `<dir>/<suite>.jsonl`, where `<dir>` is
/// `$PQUEUE_LEDGER_DIR` if set (the CI gate points every suite at one collection dir), else
/// `<repo>/target/pqueue-ledger` derived from the caller's `manifest_dir` (pass `env!("CARGO_MANIFEST_DIR")`
/// so this resolves to the repo-root `target/` regardless of which workspace the suite runs in).
///
/// NOTE: this `PQUEUE_LEDGER_DIR` read is the ONE intentional library `std::env` access in the workspace. It
/// is CI / test-evidence tooling (where validation suites drop their JSONL ledger rows), NOT server runtime
/// configuration — so it is exempt from the "no env reads in library runtime code" rule. The runtime
/// `Config` populator (`pqueue_server::Config::from_env`) is the only env→config path for the server itself.
pub fn ledger_path(manifest_dir: &str, suite: &str) -> std::path::PathBuf {
    let dir = match std::env::var("PQUEUE_LEDGER_DIR") {
        Ok(d) if !d.trim().is_empty() => {
            let path = std::path::PathBuf::from(d);
            if path.is_absolute() {
                path
            } else {
                Path::new(manifest_dir).join("../..").join(path)
            }
        }
        // `..` resolves at IO time; crates/<x>/../../target == repo-root target.
        _ => Path::new(manifest_dir).join("../../target/pqueue-ledger"),
    };
    dir.join(format!("{suite}.jsonl"))
}

/// Append one row to the ledger at `path`, creating the file (and parent dirs) if needed. The whole line —
/// JSON body AND trailing newline — is written in a SINGLE `write_all`, so under the OS append flag
/// concurrent appenders stay line-atomic for lines below the platform atomic-append size (PIPE_BUF). (A
/// `writeln!` would emit the body and `"\n"` as two separate writes, which O_APPEND could interleave.)
pub fn append_row(path: &Path, row: &LedgerRow) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    f.write_all(format!("{}\n", row.to_jsonl()).as_bytes())
}

/// A validation finding (a reason a ledger row or the ledger as a whole is not acceptable evidence).
#[derive(Debug, Clone, PartialEq)]
pub struct LedgerError(pub String);

impl std::fmt::Display for LedgerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Outcome of validating a ledger: the rows seen, the union of evidence ids RELEASE-tier rows substantiate
/// (`evidence_ids`, the only ones the headline requirement counts), and — for visibility — the evidence ids
/// only seen on `smoke`-tier rows.
#[derive(Debug, Clone, Default)]
pub struct LedgerSummary {
    pub rows: usize,
    pub evidence_ids: std::collections::BTreeSet<String>,
    pub smoke_evidence_ids: std::collections::BTreeSet<String>,
}

/// A governed TP-002 manifest names the exact ledger file authoritative for each evidence ID.
/// Broad directory scans are intentionally not supported by this format.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseManifest {
    pub schema_version: u32,
    pub authorities: Vec<ReleaseAuthority>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseAuthority {
    pub evidence_id: String,
    pub path: String,
}

/// Validate a ledger file. In `strict` mode each row must be well-formed AND acceptable evidence (exit 0,
/// non-empty identifying fields, and traceable to at least one acceptance or evidence id). Returns the
/// [`LedgerSummary`] on success, or every [`LedgerError`] found.
pub fn verify_ledger(path: &Path, strict: bool) -> Result<LedgerSummary, Vec<LedgerError>> {
    let file = match fs::File::open(path) {
        Ok(f) => f,
        Err(e) => {
            return Err(vec![LedgerError(format!(
                "cannot open ledger {path:?}: {e}"
            ))]);
        }
    };
    let mut errors = Vec::new();
    let mut summary = LedgerSummary::default();
    for (i, line) in io::BufReader::new(file).lines().enumerate() {
        let lineno = i + 1;
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                errors.push(LedgerError(format!("line {lineno}: read error: {e}")));
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let row: LedgerRow = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                errors.push(LedgerError(format!("line {lineno}: malformed row: {e}")));
                continue;
            }
        };
        summary.rows += 1;
        // Only RELEASE-tier rows count toward the headline E0–E3 requirement; smoke-tier rows are recorded
        // separately so an in-process/reduced-scale run can never satisfy a release-evidence gate.
        let ids = row.measurements.tp002_evidence_ids.iter().cloned();
        if row.evidence_tier == "smoke" {
            summary.smoke_evidence_ids.extend(ids);
        } else {
            summary.evidence_ids.extend(ids);
        }
        if strict {
            for e in strict_row_errors(&row) {
                errors.push(LedgerError(format!("line {lineno} ({}): {e}", row.suite)));
            }
        }
    }
    if strict && summary.rows == 0 {
        errors.push(LedgerError("ledger is empty".into()));
    }
    if errors.is_empty() {
        Ok(summary)
    } else {
        Err(errors)
    }
}

/// Strict-mode acceptability checks for a single row.
fn strict_row_errors(row: &LedgerRow) -> Vec<String> {
    let mut e = Vec::new();
    if row.exit_status != 0 {
        e.push(format!(
            "exit_status {} != 0 (a failed run is not evidence)",
            row.exit_status
        ));
    }
    if row.suite.trim().is_empty() {
        e.push("empty suite".into());
    }
    if row.command.trim().is_empty() {
        e.push("empty command".into());
    }
    if row.backend_profile.trim().is_empty() {
        e.push("empty backend_profile".into());
    }
    if row.scale.trim().is_empty() {
        e.push("empty scale".into());
    }
    if row.environment.trim().is_empty() {
        e.push("empty environment".into());
    }
    if row.pass_bar.trim().is_empty() {
        e.push("empty pass_bar".into());
    }
    if row.ac_ids.is_empty() && row.measurements.tp002_evidence_ids.is_empty() {
        e.push("row cites no ac_ids and no tp002_evidence_ids (untraceable)".into());
    }
    e
}

/// Validate EVERY `*.jsonl` ledger in `dir`, merging the per-file summaries (rows, release-tier
/// `evidence_ids`, and `smoke_evidence_ids`). The gate emits one file per suite into a clean dir, so this
/// aggregates the whole run. Returns the merged [`LedgerSummary`] or every [`LedgerError`] across all files.
pub fn verify_ledger_dir(dir: &Path, strict: bool) -> Result<LedgerSummary, Vec<LedgerError>> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            return Err(vec![LedgerError(format!(
                "cannot read ledger dir {dir:?}: {e}"
            ))]);
        }
    };
    let mut paths: Vec<std::path::PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
        .collect();
    paths.sort();
    let mut merged = LedgerSummary::default();
    let mut errors = Vec::new();
    for p in &paths {
        match verify_ledger(p, strict) {
            Ok(s) => {
                merged.rows += s.rows;
                merged.evidence_ids.extend(s.evidence_ids);
                merged.smoke_evidence_ids.extend(s.smoke_evidence_ids);
            }
            Err(es) => errors.extend(
                es.into_iter()
                    .map(|e| LedgerError(format!("{}: {}", p.display(), e.0))),
            ),
        }
    }
    if strict && paths.is_empty() {
        errors.push(LedgerError(format!("no *.jsonl ledger files in {dir:?}")));
    }
    if errors.is_empty() {
        Ok(merged)
    } else {
        Err(errors)
    }
}

/// Verify the exact TP-002 authority files listed by a governed release manifest.
///
/// Each authority must be one strict-valid ledger containing exactly one row. The row must claim only
/// the listed E-ID, be release-tier at release scale, report `bars_met: true`, and use the profile governed
/// for that E-ID. Manifest paths are relative to the manifest and cannot escape its directory.
pub fn verify_release_manifest(path: &Path) -> Result<LedgerSummary, Vec<LedgerError>> {
    let contents = match fs::read(path) {
        Ok(contents) => contents,
        Err(error) => {
            return Err(vec![LedgerError(format!(
                "cannot read release manifest {}: {error}",
                path.display()
            ))]);
        }
    };
    let manifest: ReleaseManifest = match serde_json::from_slice(&contents) {
        Ok(manifest) => manifest,
        Err(error) => {
            return Err(vec![LedgerError(format!(
                "malformed release manifest {}: {error}",
                path.display()
            ))]);
        }
    };

    let mut errors = Vec::new();
    if manifest.schema_version != 1 {
        errors.push(LedgerError(format!(
            "unsupported release manifest schema_version {}; expected 1",
            manifest.schema_version
        )));
    }
    if manifest.authorities.is_empty() {
        errors.push(LedgerError(
            "release manifest has no authority entries".into(),
        ));
    }

    let base = path.parent().unwrap_or_else(|| Path::new("."));
    let mut evidence_ids = std::collections::BTreeSet::new();
    let mut authority_paths = std::collections::BTreeSet::new();
    let mut rows = 0;
    for authority in manifest.authorities {
        let id = authority.evidence_id.as_str();
        if !matches!(id, "E0" | "E1" | "E2" | "E3") {
            errors.push(LedgerError(format!(
                "unknown TP-002 evidence id {:?}",
                authority.evidence_id
            )));
            continue;
        }
        if !evidence_ids.insert(authority.evidence_id.clone()) {
            errors.push(LedgerError(format!(
                "duplicate authority for evidence id {id}"
            )));
            continue;
        }
        if !safe_manifest_path(&authority.path) {
            errors.push(LedgerError(format!(
                "authority path {:?} is not a safe manifest-relative path",
                authority.path
            )));
            continue;
        }
        if !authority_paths.insert(authority.path.clone()) {
            errors.push(LedgerError(format!(
                "authority file {:?} is listed more than once",
                authority.path
            )));
            continue;
        }

        let ledger_path = base.join(&authority.path);
        match verify_ledger(&ledger_path, true) {
            Ok(summary) if summary.rows != 1 => {
                errors.push(LedgerError(format!(
                    "authority {id} file {:?} contains {} rows; expected exactly one",
                    authority.path, summary.rows
                )));
                continue;
            }
            Ok(_) => {}
            Err(file_errors) => {
                errors.extend(file_errors.into_iter().map(|error| {
                    LedgerError(format!(
                        "authority {id} file {:?}: {}",
                        authority.path, error.0
                    ))
                }));
                continue;
            }
        }

        let (row, raw_row) = match read_single_ledger_row(&ledger_path) {
            Ok(row) => row,
            Err(error) => {
                errors.push(LedgerError(format!(
                    "authority {id} file {:?}: {error}",
                    authority.path
                )));
                continue;
            }
        };
        rows += 1;
        for error in release_authority_errors(id, &row, &raw_row) {
            errors.push(LedgerError(format!(
                "authority {id} file {:?}: {error}",
                authority.path
            )));
        }
    }
    for required in ["E0", "E1", "E2", "E3"] {
        if !evidence_ids.contains(required) {
            errors.push(LedgerError(format!(
                "release manifest is missing authority for {required}"
            )));
        }
    }

    if errors.is_empty() {
        Ok(LedgerSummary {
            rows,
            evidence_ids,
            smoke_evidence_ids: Default::default(),
        })
    } else {
        Err(errors)
    }
}

fn safe_manifest_path(value: &str) -> bool {
    let path = Path::new(value);
    !value.is_empty()
        && !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn read_single_ledger_row(path: &Path) -> Result<(LedgerRow, serde_json::Value), String> {
    let contents = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let mut rows = contents.lines().filter(|line| !line.trim().is_empty());
    let line = rows
        .next()
        .ok_or_else(|| "authority ledger is empty".to_string())?;
    let raw: serde_json::Value = serde_json::from_str(line).map_err(|error| error.to_string())?;
    let row = serde_json::from_value(raw.clone()).map_err(|error| error.to_string())?;
    Ok((row, raw))
}

fn release_authority_errors(id: &str, row: &LedgerRow, raw_row: &serde_json::Value) -> Vec<String> {
    let mut errors = Vec::new();
    let explicit_tier = raw_row
        .get("evidence_tier")
        .and_then(serde_json::Value::as_str);
    if explicit_tier != Some("release") {
        errors.push(format!(
            "evidence_tier must be explicitly and exactly \"release\", got {explicit_tier:?}"
        ));
    }
    if row.scale != "release" {
        errors.push(format!(
            "scale must be exactly \"release\", got {:?}",
            row.scale
        ));
    }
    if row.measurements.tp002_evidence_ids.as_slice() != [id] {
        errors.push(format!(
            "row evidence ids {:?} do not exactly match listed authority {id}",
            row.measurements.tp002_evidence_ids
        ));
    }
    match row.measurements.values.get("bars_met") {
        Some(serde_json::Value::Bool(true)) => {}
        Some(value) => errors.push(format!("bars_met must be boolean true, got {value}")),
        None => errors.push("bars_met is required and must be boolean true".into()),
    }
    let profile_allowed = match id {
        "E0" | "E1" => row.backend_profile == "postgres_native",
        "E2" | "E3" => matches!(
            row.backend_profile.as_str(),
            "object_log_inmemory_projection" | "object_log_sqlite_projection"
        ),
        _ => false,
    };
    if !profile_allowed {
        errors.push(format!(
            "backend_profile {:?} is not governed for {id}",
            row.backend_profile
        ));
    }
    errors
}

/// Assert every id in `required` (e.g. `["E0","E1","E2","E3"]`) appears in some RELEASE-tier row's
/// `measurements.tp002_evidence_ids`. Returns the missing ids (empty = satisfied).
pub fn missing_evidence(summary: &LedgerSummary, required: &[String]) -> Vec<String> {
    required
        .iter()
        .filter(|id| !summary.evidence_ids.contains(*id))
        .cloned()
        .collect()
}

/// Like [`missing_evidence`] but against the SMOKE-tier evidence ids — for the gate's in-process smoke lane
/// (which records evidence but cannot satisfy the release headline).
pub fn missing_smoke_evidence(summary: &LedgerSummary, required: &[String]) -> Vec<String> {
    required
        .iter()
        .filter(|id| !summary.smoke_evidence_ids.contains(*id))
        .cloned()
        .collect()
}

/// TP-002 **E2** (cross-queue scale-out / ADR-008) release-bar judgment + ledger-row construction.
///
/// This is the SHARED, PURE judgment behind the TP-002 E2 verification-ledger row. The in-cluster load
/// generator (`pqueue-loadgen emit-row`) folds three per-owner-count measured scale points (owners 2/4/8)
/// into one row; this module decides whether that sweep cleared the four release bars and, if so, whether
/// the row is `release`-tier (counts toward the headline E0–E3 requirement) or `smoke`-tier. It is a pure
/// function of the measured inputs so the judgment is unit-testable from `pqueue-bench` WITHOUT provisioning
/// a live cluster.
///
/// The four E2 release bars (every value MEASURED):
/// 1. ingest aggregate strictly non-decreasing 2 → 4 → 8;
/// 2. 8-owner ingest aggregate ≥ [`SCALE_MULTIPLE_BAR`]× the 2-owner aggregate;
/// 3. worst per-queue throughput — ingest AND claim+finalize — ≥ the E0 floor ([`FLOOR_ITEMS_PER_SEC`]);
/// 4. no queue served by more than one owner (live-proven: at 8 owners, every queue is unknown on every
///    OTHER node, so the cross-node "no such queue" confirmation count equals the expected
///    `owners * queues_per_owner * (owners - 1)`).
pub mod e2 {
    use super::{LedgerRow, Measurements};
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    /// The E0 per-queue throughput floor (TP-002): 10,000,000 accepted items/hr == 2,777.78 items/s.
    pub const FLOOR_ITEMS_PER_SEC: f64 = 10_000_000.0 / 3600.0;
    /// The E2 headline cross-node multiple: the 8-owner ingest aggregate must be at least this many times the
    /// 2-owner aggregate.
    pub const SCALE_MULTIPLE_BAR: f64 = 3.5;
    /// The canonical owner counts an E2 sweep must cover (the bars compare 8 vs 2 and require 2→4→8 monotonic).
    pub const CANONICAL_OWNER_COUNTS: [usize; 3] = [2, 4, 8];

    /// One MEASURED scale point: the result of driving the segmented `object_log_sqlite_projection` workload
    /// at ONE owner count. Mirrors the load generator's per-run `RunResult` wire type (identical field names
    /// + serde shape) so the generator can use this directly as its `run`→`emit-row` wire type.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct E2ScalePoint {
        /// Owner-node count for this scale point.
        pub owners: usize,
        /// Aggregate ingest throughput (items/s) across all queues at this owner count.
        pub ingest_aggregate: f64,
        /// Worst (minimum) single-queue ingest throughput (items/s) at this owner count.
        pub ingest_min_per_queue: f64,
        /// Aggregate claim+finalize (drain) throughput (items/s) at this owner count.
        pub drain_aggregate: f64,
        /// Worst (minimum) single-queue claim+finalize throughput (items/s) at this owner count.
        pub drain_min_per_queue: f64,
        /// Cross-node "no such queue" confirmations observed (every queue rejected by every non-owner node).
        pub one_owner_confirmations: usize,
        /// Queues owned per node (disjoint across nodes).
        pub queues_per_owner: usize,
        /// Items driven per queue.
        pub items_per_queue: u64,
        /// Concurrent connections per queue.
        pub conns_per_queue: usize,
    }

    /// Per-node tuning recorded into the evidence row (passed by the orchestrator). Mirrors the load
    /// generator's `TuningMeta` wire type (identical field names + serde shape).
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct E2Tuning {
        pub segment_max_latency_ms: u64,
        pub segment_target_bytes: usize,
        pub worker_threads_per_node: usize,
        pub server_cpu_limit: String,
        pub server_cpu_request: String,
        pub loadgen_cpu_limit: String,
        pub cores: usize,
        pub kind_node_image: String,
        pub sweep: u64,
    }

    /// The judged verdict: each bar's pass/fail, the measured quantities the bars were judged from, and the
    /// AND of all four ([`bars_met`](Self::bars_met)) — the ONLY thing that promotes the row to `release`.
    #[derive(Debug, Clone, PartialEq)]
    pub struct E2Verdict {
        /// Whether the canonical owner counts (2/4/8) are all present — bars cannot pass without them.
        pub canonical_owners_present: bool,
        /// Bar (1): ingest aggregate non-decreasing 2 → 4 → 8.
        pub nondecreasing: bool,
        /// Bar (2): the measured 8-owner / 2-owner ingest aggregate ratio.
        pub ratio_8_2: f64,
        /// Bar (2): `ratio_8_2 >= SCALE_MULTIPLE_BAR`.
        pub scale_pass: bool,
        /// Bar (3): worst per-queue ingest throughput across all scale points.
        pub worst_ingest_per_queue: f64,
        /// Bar (3): worst per-queue claim+finalize throughput across all scale points.
        pub worst_drain_per_queue: f64,
        /// Bar (3): both worst-per-queue throughputs clear the E0 floor.
        pub floor_pass: bool,
        /// Bar (4): cross-node confirmations measured at 8 owners.
        pub one_owner_confirmations: usize,
        /// Bar (4): the confirmation count one-owner-per-queue requires at 8 owners.
        pub expected_confirmations: usize,
        /// Bar (4): every queue is served by exactly one owner (confirmations == expected, and queues exist).
        pub disjoint_pass: bool,
        /// The AND of all four bars. `true` ⇒ the row is release-tier; `false` ⇒ smoke-tier.
        pub bars_met: bool,
    }

    /// The cross-node "no such queue" confirmations one-owner-per-queue MUST produce at `owners` nodes each
    /// owning `queues_per_owner` queues: every one of the `owners * queues_per_owner` queues is probed on the
    /// `owners - 1` OTHER nodes and must be unknown on each. Fewer than this ⇒ some queue answered on more
    /// than one node ⇒ bar (4) fails.
    pub fn expected_one_owner_confirmations(owners: usize, queues_per_owner: usize) -> usize {
        owners * queues_per_owner * owners.saturating_sub(1)
    }

    /// Judge the four E2 release bars from the MEASURED scale points. Pure: no IO, no process exit.
    pub fn evaluate_e2_bars(points: &[E2ScalePoint]) -> E2Verdict {
        let at = |n: usize| points.iter().find(|p| p.owners == n);
        let canonical_owners_present = CANONICAL_OWNER_COUNTS.iter().all(|&n| at(n).is_some());

        // Bar (3): worst per-queue across ALL scale points (the WORST single queue, not an average), for both
        // ingest and claim+finalize. A single starved queue trips this.
        let worst_ingest_per_queue = points
            .iter()
            .map(|p| p.ingest_min_per_queue)
            .fold(f64::INFINITY, f64::min);
        let worst_drain_per_queue = points
            .iter()
            .map(|p| p.drain_min_per_queue)
            .fold(f64::INFINITY, f64::min);
        let worst_per_queue = worst_ingest_per_queue.min(worst_drain_per_queue);
        let floor_pass = worst_per_queue.is_finite() && worst_per_queue >= FLOOR_ITEMS_PER_SEC;

        let (
            nondecreasing,
            ratio_8_2,
            scale_pass,
            one_owner_confirmations,
            expected_confirmations,
            disjoint_pass,
        ) = if canonical_owners_present {
            let (p2, p4, p8) = (at(2).unwrap(), at(4).unwrap(), at(8).unwrap());
            let nondecreasing = p4.ingest_aggregate >= p2.ingest_aggregate
                && p8.ingest_aggregate >= p4.ingest_aggregate;
            let ratio_8_2 = p8.ingest_aggregate / p2.ingest_aggregate;
            let scale_pass = ratio_8_2 >= SCALE_MULTIPLE_BAR;
            let one_owner_confirmations = p8.one_owner_confirmations;
            let expected_confirmations =
                expected_one_owner_confirmations(p8.owners, p8.queues_per_owner);
            let disjoint_pass = p8.queues_per_owner > 0
                && expected_confirmations > 0
                && one_owner_confirmations == expected_confirmations;
            (
                nondecreasing,
                ratio_8_2,
                scale_pass,
                one_owner_confirmations,
                expected_confirmations,
                disjoint_pass,
            )
        } else {
            (false, 0.0, false, 0, 0, false)
        };

        let bars_met =
            canonical_owners_present && nondecreasing && scale_pass && floor_pass && disjoint_pass;

        E2Verdict {
            canonical_owners_present,
            nondecreasing,
            ratio_8_2,
            scale_pass,
            worst_ingest_per_queue,
            worst_drain_per_queue,
            floor_pass,
            one_owner_confirmations,
            expected_confirmations,
            disjoint_pass,
            bars_met,
        }
    }

    /// Build the TP-002 E2 verification-ledger row from the MEASURED scale points + tuning + the judged
    /// [`E2Verdict`]. The row's `evidence_tier`/`scale` are `release` IFF [`E2Verdict::bars_met`], else
    /// `smoke` (never a faked release row). The row shape is byte-for-byte compatible with the live evidence
    /// previously emitted by `pqueue-loadgen` (see `docs/perf/evidence/tp002-e2-multinode-kind-release.jsonl`).
    ///
    /// `points` MUST cover the canonical owner counts (2/4/8); the per-owner-count values are read by owner
    /// count.
    pub fn build_e2_row(
        points: &[E2ScalePoint],
        tuning: &E2Tuning,
        verdict: &E2Verdict,
    ) -> LedgerRow {
        let at = |n: usize| {
            points
                .iter()
                .find(|p| p.owners == n)
                .unwrap_or_else(|| panic!("build_e2_row needs a scale point for owners={n}"))
        };
        let tier = if verdict.bars_met { "release" } else { "smoke" };

        let values = BTreeMap::from([
            (
                "owners_2_ingest_aggregate_per_s".to_string(),
                serde_json::json!(at(2).ingest_aggregate.round()),
            ),
            (
                "owners_4_ingest_aggregate_per_s".to_string(),
                serde_json::json!(at(4).ingest_aggregate.round()),
            ),
            (
                "owners_8_ingest_aggregate_per_s".to_string(),
                serde_json::json!(at(8).ingest_aggregate.round()),
            ),
            (
                "owners_2_claim_finalize_aggregate_per_s".to_string(),
                serde_json::json!(at(2).drain_aggregate.round()),
            ),
            (
                "owners_4_claim_finalize_aggregate_per_s".to_string(),
                serde_json::json!(at(4).drain_aggregate.round()),
            ),
            (
                "owners_8_claim_finalize_aggregate_per_s".to_string(),
                serde_json::json!(at(8).drain_aggregate.round()),
            ),
            (
                "scale_out_8_vs_2_ingest_multiple".to_string(),
                serde_json::json!((verdict.ratio_8_2 * 100.0).round() / 100.0),
            ),
            (
                "scale_multiple_bar".to_string(),
                serde_json::json!(SCALE_MULTIPLE_BAR),
            ),
            (
                "ingest_aggregate_non_decreasing".to_string(),
                serde_json::json!(verdict.nondecreasing),
            ),
            (
                "worst_ingest_per_queue_per_s".to_string(),
                serde_json::json!(verdict.worst_ingest_per_queue.round()),
            ),
            (
                "worst_claim_finalize_per_queue_per_s".to_string(),
                serde_json::json!(verdict.worst_drain_per_queue.round()),
            ),
            (
                "e0_floor_per_s".to_string(),
                serde_json::json!(FLOOR_ITEMS_PER_SEC.round()),
            ),
            (
                "one_owner_per_queue_confirmations".to_string(),
                serde_json::json!(verdict.one_owner_confirmations),
            ),
            (
                "queues_per_owner".to_string(),
                serde_json::json!(at(8).queues_per_owner),
            ),
            (
                "items_per_queue".to_string(),
                serde_json::json!(at(8).items_per_queue),
            ),
            (
                "conns_per_queue".to_string(),
                serde_json::json!(at(8).conns_per_queue),
            ),
            (
                "segment_max_latency_ms".to_string(),
                serde_json::json!(tuning.segment_max_latency_ms),
            ),
            (
                "segment_target_bytes".to_string(),
                serde_json::json!(tuning.segment_target_bytes),
            ),
            (
                "worker_threads_per_node".to_string(),
                serde_json::json!(tuning.worker_threads_per_node),
            ),
            (
                "server_cpu_limit".to_string(),
                serde_json::json!(tuning.server_cpu_limit),
            ),
            (
                "server_cpu_request".to_string(),
                serde_json::json!(tuning.server_cpu_request),
            ),
            (
                "loadgen_cpu_limit".to_string(),
                serde_json::json!(tuning.loadgen_cpu_limit),
            ),
            (
                "kind_node_image".to_string(),
                serde_json::json!(tuning.kind_node_image),
            ),
            ("sweep".to_string(), serde_json::json!(tuning.sweep)),
            ("cores".to_string(), serde_json::json!(tuning.cores)),
            ("bars_met".to_string(), serde_json::json!(verdict.bars_met)),
        ]);

        LedgerRow {
            suite: "performance_multi_node_object_log_e2_kind".into(),
            command: "scripts/perf/tp002-e2-kind.sh (pqueue-loadgen run -> emit-row; kind: CPU-limited server pods + lean in-cluster load Job)".into(),
            backend_profile: "object_log_sqlite_projection".into(),
            scale: tier.into(),
            seed: 0,
            environment: format!(
                "live multi-node ADR-008 owner cluster on a kind (Kubernetes-in-docker) cluster; \
                 {cores} cores; node image {node_image}; owner counts 2/4/8; each owner an independent \
                 pqueue-service Deployment(replicas=1)+Service on object_log_sqlite_projection in SEGMENTED \
                 group-commit mode (TD-004) with its own object-log root + sqlite projection on an emptyDir \
                 medium=Memory tmpfs, distinct PQUEUE_NODE_ID, disjoint PQUEUE_BOOTSTRAP_QUEUES, CPU \
                 request={req}/limit={lim}, {worker} worker threads; load driven by a LEAN, SEPARATED \
                 in-cluster Job (CPU limit {load}) speaking raw RESP pod->pod over Service ClusterIP to each \
                 owner; each queue driven by {conns} concurrent connections",
                cores = tuning.cores,
                node_image = tuning.kind_node_image,
                req = tuning.server_cpu_request,
                lim = tuning.server_cpu_limit,
                worker = tuning.worker_threads_per_node,
                load = tuning.loadgen_cpu_limit,
                conns = at(8).conns_per_queue,
            ),
            exit_status: 0,
            ac_ids: vec![],
            inv_ids: vec![],
            pass_bar: "E2: ingest aggregate strictly non-decreasing 2->4->8; 8-owner ingest aggregate >= 3.5x 2-owner; worst per-queue ingest AND claim+finalize each >= E0 floor (2777.78/s); no queue served by more than one owner".into(),
            evidence_tier: tier.into(),
            measurements: Measurements {
                tp002_evidence_ids: vec!["E2".into()],
                values,
            },
        }
    }
}

/// TP-002 **E3 cost model** (ADR-001 "Napkin Cost Comparison" → release evidence).
///
/// ADR-001 asserts, *directionally*, that the `object_log_sqlite_projection` backend has a lower
/// $/command than an always-on relational authority at high volume, because batched object-storage commits
/// (request-priced PUTs + cheap `$/GB-month` storage, **no** per-I/O or provisioned-IOPS charge) beat a
/// provisioned database instance that must hold the resident backlog and sustain the high-churn
/// `SKIP LOCKED` claim index. This module turns that direction into a reproducible, fixture-tested
/// **calculation**: it scales the REAL E3 measured object/segment counts to a billion commands, prices
/// them against cited inputs, prices the `postgres_native` baseline (instance-hours at the measured E0
/// throughput + storage + provisioned IOPS), and returns a structured [`CostComparison`].
///
/// It is a PURE function of its inputs — no IO, no process exit — so the comparison is unit-testable from a
/// fixture and the calculator can be shown to RESPOND to its inputs (crank a price until the crossover) rather
/// than being hard-wired to a conclusion.
///
/// ## Apples-to-apples (the honesty bar)
///
/// `object_log_sqlite_projection` ALSO runs a compute node (it batches commands into segments and projects
/// them into SQLite), so this is NOT "free S3 vs a paid DB". Both sides are charged compute for the same
/// always-on billing window. The legitimate, modelled win is two-fold and each is a separate, inspectable
/// line item:
/// 1. **Durable storage + I/O**: the durable log lives on object storage (`$/GB-month` + request-priced
///    PUTs, *no per-I/O charge*) instead of DB storage + **provisioned IOPS** sized for the claim-index
///    churn (the MVCC-bloat finding in `docs/perf/tp002-e0e1-postgres-release-10m.md` documents how
///    IOPS-bound that path is).
/// 2. **Node sizing**: the object-log node can be smaller/cheaper than the IOPS-bound claim authority — but
///    this is exposed as a separate price input so a reviewer can set both nodes equal and confirm the win
///    survives on the storage/I/O term alone.
pub mod cost {
    use super::{LedgerRow, Measurements};
    use std::collections::BTreeMap;

    /// One billion — the command count every cost figure is normalized to (`$/billion-commands`).
    pub const BILLION: f64 = 1_000_000_000.0;
    /// Hours in a 30-day month (`30 * 24 + 10`… AWS bills `$/GB-month` against 730 hours).
    pub const HOURS_PER_MONTH: f64 = 730.0;
    /// Bytes per **decimal** GB — cloud `$/GB-month` and `$/GB` pricing is decimal (10^9), not GiB.
    pub const BYTES_PER_GB: f64 = 1_000_000_000.0;

    /// Cited price inputs (US-East-1). Defaults come from [`PriceInputs::adr_001_us_east_1`]; every default is
    /// traceable to ADR-001's "Napkin Cost Comparison" cited offer-file set except the EBS provisioned-IOPS
    /// unit price, which ADR-001 does not cite and is noted as such ([`Self::iops_source`]).
    #[derive(Debug, Clone, PartialEq)]
    pub struct PriceInputs {
        /// S3 Standard storage, `$/GB-month`.
        pub s3_storage_per_gb_month: f64,
        /// S3 PUT/COPY/POST/LIST, `$/1000 requests`.
        pub s3_put_per_1k: f64,
        /// S3 GET, `$/1000 requests`.
        pub s3_get_per_1k: f64,
        /// S3 DELETE/CANCEL, `$/1000 requests`. S3 Standard prices these at zero; keep the input explicit so
        /// request-accounting rows can show that deletes are tracked rather than ignored.
        pub s3_delete_per_1k: f64,
        /// The `postgres_native` provisioned DB instance, `$/hour` (the always-on claim authority).
        pub pg_instance_per_hour: f64,
        /// DB storage, `$/GB-month`.
        pub pg_storage_per_gb_month: f64,
        /// Provisioned IOPS, `$/IOPS-month` (one provisioned I/O operation per second for a month).
        pub pg_iops_per_month_each: f64,
        /// The `object_log_sqlite_projection` compute node, `$/hour` (it batches + projects; can be smaller).
        pub objectlog_node_per_hour: f64,
        /// Provenance of the S3/DB instance/storage prices.
        pub instance_source: &'static str,
        /// Provenance of the provisioned-IOPS unit price (NOT cited by ADR-001 — stated honestly).
        pub iops_source: &'static str,
    }

    impl PriceInputs {
        /// ADR-001's cited US-East-1 inputs (S3 Standard; Aurora PostgreSQL `db.r7g.large` standard as the
        /// `postgres_native` instance; EC2 `i4i.large` — NVMe-backed, suits the SQLite projection + segment
        /// buffer — as the object-log node). The provisioned-IOPS unit price is AWS EBS `io2` first-tier,
        /// which ADR-001 does not cite; it is flagged in [`Self::iops_source`].
        pub fn adr_001_us_east_1() -> Self {
            PriceInputs {
                s3_storage_per_gb_month: 0.023,
                s3_put_per_1k: 0.005,
                s3_get_per_1k: 0.0004,
                s3_delete_per_1k: 0.0,
                pg_instance_per_hour: 0.276,
                pg_storage_per_gb_month: 0.10,
                pg_iops_per_month_each: 0.065,
                objectlog_node_per_hour: 0.172,
                instance_source: "ADR-001 Napkin Cost Comparison, US-East-1: AWS S3 pricing (AmazonS3 offer file pub. \
                     2026-05-28); Aurora PostgreSQL db.r7g.large standard $0.276/hr + $0.10/GB-mo storage \
                     (AmazonRDS offer file pub. 2026-06-05); EC2 i4i.large $0.172/hr (AmazonEC2 offer file \
                     pub. 2026-06-04)",
                iops_source: "AWS EBS io2 provisioned-IOPS first tier $0.065/IOPS-month (AWS EBS pricing page, \
                     accessed 2026-06-29) — NOT cited by ADR-001; stated as the one non-ADR price input",
            }
        }
    }

    /// Measured object-log counts the cost scales to a billion commands. The headline fixture uses the REAL
    /// E3 numbers from `docs/perf/evidence/tp002-e3-objectlog-minio-release.jsonl`; the production-fill
    /// constructors model segments filled to their byte target (the E3 segments were latency-bound and small,
    /// which OVER-states PUT cost — see [`Self::e3_size_dominant`]).
    #[derive(Debug, Clone, PartialEq)]
    pub struct ObjectLogCounts {
        /// A short label for the scenario (appears in the artifact's sensitivity table).
        pub label: String,
        /// Commands committed in the measured (or modelled) sample.
        pub commands: f64,
        /// Objects PUT for those commands (segment object + manifest object per seal in E3 ⇒ 2/segment).
        pub objects_put: f64,
        /// Segments sealed for those commands.
        pub segments_sealed: f64,
    }

    impl ObjectLogCounts {
        /// REAL E3 size-dominant config (`target_bytes=4096, max_latency=1000ms`): 2048 commands → 34 segments,
        /// 68 objects. These segments sealed mostly on the latency cap with small synthetic commands, so the
        /// objects-per-command ratio is HIGHER (worse for cost) than a throughput-saturated production run that
        /// fills segments to their byte target — i.e. this is the pessimistic-but-measured case.
        pub fn e3_size_dominant() -> Self {
            ObjectLogCounts {
                label: "E3 measured (size-dominant: 4 KiB target / 1000 ms cap)".into(),
                commands: 2048.0,
                objects_put: 68.0,
                segments_sealed: 34.0,
            }
        }

        /// REAL E3 latency-dominant config (`target_bytes=8 MiB, max_latency=50ms`): 2048 commands → 50 segments,
        /// 100 objects. The tighter 50 ms cap seals more, smaller segments ⇒ the highest measured PUT-per-command.
        pub fn e3_latency_dominant() -> Self {
            ObjectLogCounts {
                label: "E3 measured (latency-dominant: 8 MiB target / 50 ms cap)".into(),
                commands: 2048.0,
                objects_put: 100.0,
                segments_sealed: 50.0,
            }
        }

        /// Production-fill model: segments filled to `target_bytes` with `bytes_per_command`-sized commands,
        /// `objects_per_segment` objects per seal (2 in E3: one segment object + one manifest object). This is
        /// what a throughput-saturated owner produces (ADR-001's "16 MiB segments ⇒ <\$2 in PUTs" case).
        pub fn filled(
            label: impl Into<String>,
            target_bytes: f64,
            bytes_per_command: f64,
            objects_per_segment: f64,
        ) -> Self {
            let commands_per_segment = (target_bytes / bytes_per_command).max(1.0);
            let segments = 1000.0; // arbitrary sample; only the ratios objects/cmd & seg/cmd are used
            ObjectLogCounts {
                label: label.into(),
                commands: commands_per_segment * segments,
                objects_put: objects_per_segment * segments,
                segments_sealed: segments,
            }
        }

        /// Mean commands per sealed segment (segment fill, for display).
        pub fn commands_per_segment(&self) -> f64 {
            self.commands / self.segments_sealed
        }
    }

    /// Workload + retention/recovery assumptions, and the MEASURED `postgres_native` E0 throughput. Defaults
    /// from [`WorkloadAssumptions::tp002_high_volume_baseline`].
    #[derive(Debug, Clone, PartialEq)]
    pub struct WorkloadAssumptions {
        /// Compute billing window, hours. Default 730 (an always-on month): the queue's DB instance and the
        /// object-log node both run continuously to hold the resident backlog and serve live traffic — you do
        /// not tear the queue authority down between batches. [`CostBreakdown`] also reports the
        /// `processing_hours` it takes to push a billion commands through at the measured throughput, which
        /// confirms one always-on instance has ample headroom.
        pub billing_window_hours: f64,
        /// Logical bytes per durable command record (ADR-001 baseline: 1 KiB encoded record).
        pub bytes_per_command: f64,
        /// Commands per fully-processed item (push + claim + finalize ⇒ 3); used to fold the measured E0
        /// ingest and claim+finalize item rates into an end-to-end command throughput.
        pub commands_per_item: f64,
        /// Resident durable working set (items) the backend must retain. Default 10,000,000 — the E0/E3 shape.
        pub resident_items: f64,
        /// Index/tuple overhead multiplier on the relational store's resident bytes (heap + claim/priority
        /// indexes + idempotency). Object storage retains the projection snapshot without this DB overhead.
        pub pg_index_overhead: f64,
        /// Provisioned IOPS the `postgres_native` claim-index churn must reserve to stay off the IOPS floor
        /// (the MVCC-bloat finding shows the drain is read-IOPS-bound). Set to 0 to model free local disk.
        pub pg_provisioned_iops: f64,
        /// Durable-log recovery window, hours: how much committed log object storage retains *behind* the
        /// latest snapshot so a node can rebuild. Object-log storage cost is tied to THIS, not total history.
        pub recovery_window_hours: f64,
        /// How many full snapshot+tail recoveries happen per billing window (drives recovery GET volume).
        pub recoveries_per_window: f64,
        /// MEASURED E0 ingest throughput, items/s (`docs/perf/tp002-e0e1-postgres-release-10m.md`).
        pub pg_ingest_per_s: f64,
        /// MEASURED E0 claim+finalize (drain) throughput, items/s.
        pub pg_claim_finalize_per_s: f64,
    }

    impl WorkloadAssumptions {
        /// The TP-002 high-volume baseline: an always-on month, 1 KiB records, 10M resident, a 24 h recovery
        /// window, and the MEASURED E0 throughputs (ingest 20,431/s, claim+finalize 6,145/s). The provisioned
        /// IOPS default (12,000) reserves headroom for the claim-index churn the E0 evidence documents.
        pub fn tp002_high_volume_baseline() -> Self {
            WorkloadAssumptions {
                billing_window_hours: HOURS_PER_MONTH,
                bytes_per_command: 1024.0,
                commands_per_item: 3.0,
                resident_items: 10_000_000.0,
                pg_index_overhead: 2.5,
                pg_provisioned_iops: 12_000.0,
                recovery_window_hours: 24.0,
                recoveries_per_window: 1.0,
                pg_ingest_per_s: 20_431.0,
                pg_claim_finalize_per_s: 6_145.0,
            }
        }

        /// End-to-end command throughput (commands/s) folded from the measured per-item E0 rates: each item is
        /// one push (at the ingest rate) plus a claim+finalize pair (at the drain rate); the per-item wall time
        /// is the sum, and the command rate is `commands_per_item / per_item_seconds`.
        pub fn pg_command_throughput_per_s(&self) -> f64 {
            let per_item_seconds = 1.0 / self.pg_ingest_per_s + 1.0 / self.pg_claim_finalize_per_s;
            self.commands_per_item / per_item_seconds
        }
    }

    /// The itemized cost of ONE backend for a billion commands. Every line is a separate, inspectable term.
    #[derive(Debug, Clone, PartialEq)]
    pub struct CostBreakdown {
        /// Object-log: PUT requests scaled to a billion commands. Postgres: 0.
        pub put_requests: f64,
        /// Object-log: cost of those PUTs. Postgres: 0.
        pub put_cost: f64,
        /// Object-log: recovery GET requests over the billing window. Postgres: 0.
        pub get_requests: f64,
        /// Object-log: cost of those recovery GETs. Postgres: 0.
        pub get_cost: f64,
        /// Durable bytes retained (GB): object-log = snapshot + recovery-window log; postgres = resident heap
        /// + index overhead.
        pub storage_gb: f64,
        /// Cost of the retained storage over the billing window.
        pub storage_cost: f64,
        /// Provisioned IOPS reserved (postgres only).
        pub provisioned_iops: f64,
        /// Cost of the provisioned IOPS over the billing window (postgres only).
        pub iops_cost: f64,
        /// Compute node hours billed (the always-on window).
        pub compute_hours: f64,
        /// Hours to push a billion commands through at the measured throughput (utilization check; ≤ window).
        pub processing_hours: f64,
        /// Cost of the compute node over the billing window.
        pub compute_cost: f64,
        /// Sum of every line above — the backend's `$/billion-commands`.
        pub total: f64,
    }

    /// The structured comparison: each backend's `$/billion-commands` with full breakdown, the ratio, and which
    /// side wins under the supplied inputs.
    #[derive(Debug, Clone, PartialEq)]
    pub struct CostComparison {
        /// `object_log_sqlite_projection` total `$/billion-commands`.
        pub objectlog_per_billion: f64,
        /// `postgres_native` total `$/billion-commands`.
        pub postgres_per_billion: f64,
        /// `postgres_per_billion / objectlog_per_billion` (> 1 ⇒ object-log is cheaper, by this multiple).
        pub ratio: f64,
        /// `true` iff `objectlog_per_billion < postgres_per_billion` under these inputs (NOT hard-coded).
        pub objectlog_wins: bool,
        /// End-to-end postgres command throughput used for `processing_hours` (commands/s).
        pub pg_command_throughput_per_s: f64,
        /// Itemized object-log cost.
        pub objectlog: CostBreakdown,
        /// Itemized postgres cost.
        pub postgres: CostBreakdown,
    }

    /// Compute the `$/billion-commands` comparison. Pure: a deterministic function of `(counts, workload,
    /// prices)`, no IO.
    pub fn compute_comparison(
        counts: &ObjectLogCounts,
        w: &WorkloadAssumptions,
        p: &PriceInputs,
    ) -> CostComparison {
        let month_fraction = w.billing_window_hours / HOURS_PER_MONTH;

        // ----- object_log_sqlite_projection -----
        let objects_per_command = counts.objects_put / counts.commands;
        let put_requests = objects_per_command * BILLION;
        let put_cost = put_requests / 1000.0 * p.s3_put_per_1k;

        // Durable storage: the projection snapshot (resident working set) + the committed log retained behind
        // it for the recovery window. The command rate at a billion-per-window sets how much log a window holds.
        let snapshot_bytes = w.resident_items * w.bytes_per_command;
        let command_rate_per_hour = BILLION / w.billing_window_hours;
        let recovery_log_commands = command_rate_per_hour * w.recovery_window_hours;
        let recovery_log_bytes = recovery_log_commands * w.bytes_per_command;
        let ol_storage_gb = (snapshot_bytes + recovery_log_bytes) / BYTES_PER_GB;
        let ol_storage_cost = ol_storage_gb * p.s3_storage_per_gb_month * month_fraction;

        // Recovery GETs: rebuild reads the snapshot (≈1 manifest+snapshot fetch) plus the recovery-window
        // segments, once per recovery. Tiny next to PUTs but modelled for completeness.
        let recovery_segments = recovery_log_commands / counts.commands_per_segment();
        let get_requests = (recovery_segments + 1.0) * w.recoveries_per_window;
        let get_cost = get_requests / 1000.0 * p.s3_get_per_1k;

        let ol_compute_hours = w.billing_window_hours;
        let processing_hours = BILLION / w.pg_command_throughput_per_s() / 3600.0;
        let ol_compute_cost = ol_compute_hours * p.objectlog_node_per_hour;

        let objectlog = CostBreakdown {
            put_requests,
            put_cost,
            get_requests,
            get_cost,
            storage_gb: ol_storage_gb,
            storage_cost: ol_storage_cost,
            provisioned_iops: 0.0,
            iops_cost: 0.0,
            compute_hours: ol_compute_hours,
            processing_hours,
            compute_cost: ol_compute_cost,
            total: put_cost + get_cost + ol_storage_cost + ol_compute_cost,
        };

        // ----- postgres_native -----
        let pg_storage_gb =
            w.resident_items * w.bytes_per_command * w.pg_index_overhead / BYTES_PER_GB;
        let pg_storage_cost = pg_storage_gb * p.pg_storage_per_gb_month * month_fraction;
        let pg_iops_cost = w.pg_provisioned_iops * p.pg_iops_per_month_each * month_fraction;
        let pg_compute_cost = w.billing_window_hours * p.pg_instance_per_hour;

        let postgres = CostBreakdown {
            put_requests: 0.0,
            put_cost: 0.0,
            get_requests: 0.0,
            get_cost: 0.0,
            storage_gb: pg_storage_gb,
            storage_cost: pg_storage_cost,
            provisioned_iops: w.pg_provisioned_iops,
            iops_cost: pg_iops_cost,
            compute_hours: w.billing_window_hours,
            processing_hours,
            compute_cost: pg_compute_cost,
            total: pg_compute_cost + pg_storage_cost + pg_iops_cost,
        };

        let ratio = postgres.total / objectlog.total;
        CostComparison {
            objectlog_per_billion: objectlog.total,
            postgres_per_billion: postgres.total,
            ratio,
            objectlog_wins: objectlog.total < postgres.total,
            pg_command_throughput_per_s: w.pg_command_throughput_per_s(),
            objectlog,
            postgres,
        }
    }

    /// Build the TP-002 E3 **cost-model** ledger row from a computed comparison. The row is **smoke-tier**: it
    /// is a derived CALCULATION over the measured E3/E0 counts (cited prices, stated assumptions), NOT a fresh
    /// live measurement — so it is recorded and strict-validated for visibility but never counts as headline
    /// release evidence on its own (the live MinIO E3 run carries the release-tier `E3`). It is traceable
    /// (`tp002_evidence_ids=["E3"]`) and carries the computed numbers + the inputs that produced them.
    pub fn build_cost_row(
        comparison: &CostComparison,
        counts: &ObjectLogCounts,
        w: &WorkloadAssumptions,
        p: &PriceInputs,
        command: &str,
    ) -> LedgerRow {
        let round2 = |x: f64| (x * 100.0).round() / 100.0;
        let values = BTreeMap::from([
            ("cost_model".to_string(), serde_json::json!(true)),
            (
                "objectlog_usd_per_billion_commands".to_string(),
                serde_json::json!(round2(comparison.objectlog_per_billion)),
            ),
            (
                "postgres_usd_per_billion_commands".to_string(),
                serde_json::json!(round2(comparison.postgres_per_billion)),
            ),
            (
                "postgres_over_objectlog_ratio".to_string(),
                serde_json::json!(round2(comparison.ratio)),
            ),
            (
                "objectlog_below_postgres".to_string(),
                serde_json::json!(comparison.objectlog_wins),
            ),
            (
                "objectlog_put_cost_usd".to_string(),
                serde_json::json!(round2(comparison.objectlog.put_cost)),
            ),
            (
                "objectlog_node_compute_usd".to_string(),
                serde_json::json!(round2(comparison.objectlog.compute_cost)),
            ),
            (
                "objectlog_storage_usd".to_string(),
                serde_json::json!(round2(comparison.objectlog.storage_cost)),
            ),
            (
                "postgres_compute_usd".to_string(),
                serde_json::json!(round2(comparison.postgres.compute_cost)),
            ),
            (
                "postgres_provisioned_iops_usd".to_string(),
                serde_json::json!(round2(comparison.postgres.iops_cost)),
            ),
            (
                "postgres_processing_hours_per_billion".to_string(),
                serde_json::json!(round2(comparison.postgres.processing_hours)),
            ),
            (
                "objectlog_counts_label".to_string(),
                serde_json::json!(counts.label),
            ),
            (
                "objects_per_command".to_string(),
                serde_json::json!(round2(counts.objects_put / counts.commands * 1000.0) / 1000.0),
            ),
            (
                "billing_window_hours".to_string(),
                serde_json::json!(w.billing_window_hours),
            ),
            (
                "recovery_window_hours".to_string(),
                serde_json::json!(w.recovery_window_hours),
            ),
            (
                "pg_provisioned_iops".to_string(),
                serde_json::json!(w.pg_provisioned_iops),
            ),
            (
                "price_source".to_string(),
                serde_json::json!(p.instance_source),
            ),
            (
                "iops_price_source".to_string(),
                serde_json::json!(p.iops_source),
            ),
        ]);

        LedgerRow {
            suite: "tp002_e3_cost_model".into(),
            command: command.into(),
            backend_profile: "object_log_sqlite_projection".into(),
            scale: "smoke".into(),
            seed: 0,
            environment: format!(
                "derived cost model (pqueue-cost-model): REAL E3 counts ({label}: {cmds} commands, \
                 {objs} objects, {segs} segments) scaled to 1e9 commands vs postgres_native at the measured \
                 E0 throughput ({tput:.0} commands/s); cited prices [{src}]; always-on {win}h window, \
                 {iops} provisioned IOPS",
                label = counts.label,
                cmds = counts.commands,
                objs = counts.objects_put,
                segs = counts.segments_sealed,
                tput = comparison.pg_command_throughput_per_s,
                src = p.instance_source,
                win = w.billing_window_hours,
                iops = w.pg_provisioned_iops,
            ),
            exit_status: 0,
            ac_ids: vec![],
            inv_ids: vec![],
            pass_bar:
                "E3 cost model: object_log_sqlite_projection $/billion-commands < postgres_native \
                 $/billion-commands at the documented high-volume baseline with cited prices"
                    .into(),
            evidence_tier: "smoke".into(),
            measurements: Measurements {
                tp002_evidence_ids: vec!["E3".into()],
                values,
            },
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// FIXTURE 1 — the REAL E3 size-dominant measured counts + the cited ADR-001 prices at the documented
        /// high-volume baseline: object_log_sqlite_projection is below postgres_native, and the numbers land
        /// where the hand calculation says.
        #[test]
        fn real_e3_counts_objectlog_below_postgres() {
            let counts = ObjectLogCounts::e3_size_dominant();
            let w = WorkloadAssumptions::tp002_high_volume_baseline();
            let p = PriceInputs::adr_001_us_east_1();
            let c = compute_comparison(&counts, &w, &p);

            // The headline claim: object-log is below postgres at the high-volume baseline.
            assert!(
                c.objectlog_wins,
                "object_log should be below postgres: ol={:.2} pg={:.2}",
                c.objectlog_per_billion, c.postgres_per_billion
            );
            assert!(c.ratio > 3.0, "expected >3x, got {:.2}x", c.ratio);

            // PUTs: 68/2048 objects/command * 1e9 / 1000 * $0.005 ≈ $166.
            assert!(
                (c.objectlog.put_cost - 166.02).abs() < 1.0,
                "put_cost={:.2}",
                c.objectlog.put_cost
            );
            // Postgres is dominated by its always-on instance + provisioned IOPS, not compute-time.
            assert!(
                (c.postgres.compute_cost - 201.48).abs() < 1.0,
                "pg compute={:.2}",
                c.postgres.compute_cost
            );
            assert!(
                (c.postgres.iops_cost - 780.0).abs() < 1.0,
                "pg iops={:.2}",
                c.postgres.iops_cost
            );
            // The instance-hours utilization check: one always-on instance has ample headroom (a billion
            // commands take ~20 h of a 730 h month at the measured throughput).
            assert!(
                c.postgres.processing_hours < 25.0 && c.postgres.processing_hours > 15.0,
                "processing_hours={:.2}",
                c.postgres.processing_hours
            );
        }

        /// FIXTURE 2 — the calculator RESPONDS to inputs (it is not hard-wired to "object-log wins"):
        /// cranking the S3 PUT price crosses the result over to postgres, and PUT cost is monotonic in price.
        #[test]
        fn crossover_when_put_price_cranked() {
            let counts = ObjectLogCounts::e3_size_dominant();
            let w = WorkloadAssumptions::tp002_high_volume_baseline();
            let base = PriceInputs::adr_001_us_east_1();
            let baseline = compute_comparison(&counts, &w, &base);
            assert!(baseline.objectlog_wins);

            // Crank S3 PUT 10x: object-log now exceeds postgres ⇒ the win flips. Not hard-coded.
            let mut dear = base.clone();
            dear.s3_put_per_1k = base.s3_put_per_1k * 10.0;
            let crossed = compute_comparison(&counts, &w, &dear);
            assert!(
                !crossed.objectlog_wins,
                "10x PUT price should flip the result: ol={:.2} pg={:.2}",
                crossed.objectlog_per_billion, crossed.postgres_per_billion
            );
            // Monotonic: higher PUT price ⇒ strictly higher object-log total.
            assert!(crossed.objectlog_per_billion > baseline.objectlog_per_billion);
        }

        /// FIXTURE 3 — the real crossover the artifact reports: with the postgres IOPS floor removed (free
        /// local disk) AND the pessimistic small E3 segments, postgres wins; filling segments to a production
        /// byte target flips object-log back ahead even at zero postgres IOPS. Proves the win is earned by the
        /// modelled terms, not assumed.
        #[test]
        fn iops_floor_and_segment_fill_drive_the_crossover() {
            let p = PriceInputs::adr_001_us_east_1();
            let mut w_no_iops = WorkloadAssumptions::tp002_high_volume_baseline();
            w_no_iops.pg_provisioned_iops = 0.0;

            // Zero postgres IOPS + tiny latency-bound E3 segments ⇒ postgres is the cheaper side.
            let tiny = compute_comparison(&ObjectLogCounts::e3_size_dominant(), &w_no_iops, &p);
            assert!(
                !tiny.objectlog_wins,
                "no-IOPS postgres should beat tiny-segment object-log: ol={:.2} pg={:.2}",
                tiny.objectlog_per_billion, tiny.postgres_per_billion
            );

            // Fill segments to 16 MiB at 1 KiB/command (2 objects/segment) ⇒ object-log wins even at 0 IOPS.
            let filled =
                ObjectLogCounts::filled("16 MiB fill", 16.0 * 1024.0 * 1024.0, 1024.0, 2.0);
            let big = compute_comparison(&filled, &w_no_iops, &p);
            assert!(
                big.objectlog_wins,
                "filled segments should beat no-IOPS postgres: ol={:.2} pg={:.2}",
                big.objectlog_per_billion, big.postgres_per_billion
            );
        }

        /// The win survives even when the object-log node is priced IDENTICALLY to the postgres instance —
        /// i.e. the apples-to-apples win does not depend on cherry-picking a smaller node; the storage/I/O term
        /// carries it.
        #[test]
        fn win_survives_equal_node_pricing() {
            let counts = ObjectLogCounts::e3_size_dominant();
            let w = WorkloadAssumptions::tp002_high_volume_baseline();
            let mut p = PriceInputs::adr_001_us_east_1();
            p.objectlog_node_per_hour = p.pg_instance_per_hour; // same node both sides
            let c = compute_comparison(&counts, &w, &p);
            assert!(
                c.objectlog_wins,
                "win must survive equal node pricing: ol={:.2} pg={:.2}",
                c.objectlog_per_billion, c.postgres_per_billion
            );
        }

        /// The folded command throughput matches the hand calculation from the measured E0 item rates.
        #[test]
        fn command_throughput_folds_measured_e0_rates() {
            let w = WorkloadAssumptions::tp002_high_volume_baseline();
            // 3 / (1/20431 + 1/6145) ≈ 14,173 commands/s.
            let t = w.pg_command_throughput_per_s();
            assert!((t - 14_172.6).abs() < 5.0, "throughput={t:.1}");
        }

        /// The smoke-tier cost row is traceable and strict-valid, and never masquerades as release evidence.
        #[test]
        fn cost_row_is_smoke_tier_and_traceable() {
            let counts = ObjectLogCounts::e3_size_dominant();
            let w = WorkloadAssumptions::tp002_high_volume_baseline();
            let p = PriceInputs::adr_001_us_east_1();
            let c = compute_comparison(&counts, &w, &p);
            let row = build_cost_row(&c, &counts, &w, &p, "pqueue-cost-model");
            assert_eq!(row.evidence_tier, "smoke");
            assert_eq!(row.measurements.tp002_evidence_ids, vec!["E3".to_string()]);
            assert!(super::super::strict_row_errors(&row).is_empty());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(suite: &str, exit: i32, evidence: &[&str]) -> LedgerRow {
        LedgerRow {
            suite: suite.into(),
            command: format!("cargo test {suite}"),
            backend_profile: "memory".into(),
            scale: "smoke".into(),
            seed: 7,
            environment: "in-process".into(),
            exit_status: exit,
            ac_ids: vec!["AC-E2E-1".into()],
            inv_ids: vec!["INV-1".into()],
            pass_bar: "floor held".into(),
            evidence_tier: "release".into(),
            measurements: Measurements {
                tp002_evidence_ids: evidence.iter().map(|s| s.to_string()).collect(),
                values: BTreeMap::from([("items_per_sec".into(), serde_json::json!(123456))]),
            },
        }
    }

    #[test]
    fn smoke_tier_evidence_does_not_count_toward_the_headline() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("pq-tier-{}.jsonl", std::process::id()));
        let _ = fs::remove_file(&path);
        // A release E2 row and a SMOKE E3 row.
        append_row(&path, &row("release_e2", 0, &["E2"])).unwrap();
        let mut smoke = row("smoke_e3", 0, &["E3"]);
        smoke.evidence_tier = "smoke".into();
        append_row(&path, &smoke).unwrap();

        let s = verify_ledger(&path, true).unwrap();
        // Only the release E2 counts as headline evidence; the smoke E3 is tracked separately.
        assert!(s.evidence_ids.contains("E2") && !s.evidence_ids.contains("E3"));
        assert!(s.smoke_evidence_ids.contains("E3"));
        // A gate requiring E3 is NOT satisfied by the smoke row.
        assert_eq!(
            missing_evidence(&s, &["E3".to_string()]),
            vec!["E3".to_string()]
        );
        // A legacy row that OMITS evidence_tier deserializes as release (back-compat).
        let legacy = r#"{"suite":"s","command":"c","backend_profile":"memory","scale":"release","seed":1,"environment":"ci","exit_status":0,"pass_bar":"p","measurements":{"tp002_evidence_ids":["E0"]}}"#;
        let parsed: LedgerRow = serde_json::from_str(legacy).unwrap();
        assert_eq!(parsed.evidence_tier, "release");
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn row_round_trips_jsonl() {
        let r = row("s", 0, &["E2"]);
        let parsed: LedgerRow = serde_json::from_str(&r.to_jsonl()).unwrap();
        assert_eq!(r, parsed);
        // The flattened measurement value survives the round-trip.
        assert_eq!(
            parsed.measurements.values["items_per_sec"],
            serde_json::json!(123456)
        );
    }

    #[test]
    fn strict_rejects_failed_and_untraceable_rows() {
        // A well-formed, traceable, exit-0 row has no strict errors.
        assert!(strict_row_errors(&row("ok", 0, &["E0"])).is_empty());
        // exit_status != 0.
        assert!(
            strict_row_errors(&row("bad", 1, &["E0"]))
                .iter()
                .any(|e| e.contains("exit_status"))
        );
        // no ac_ids and no evidence ids.
        let mut untraceable = row("u", 0, &[]);
        untraceable.ac_ids.clear();
        assert!(
            strict_row_errors(&untraceable)
                .iter()
                .any(|e| e.contains("untraceable"))
        );
    }

    #[test]
    fn ledger_path_honors_env_override_and_default() {
        // SAFETY: single-threaded test; we set then clear the override around the assertions.
        unsafe { std::env::set_var("PQUEUE_LEDGER_DIR", "/tmp/pq-ledger") };
        assert_eq!(
            ledger_path("/repo/crates/x", "suite_a"),
            std::path::PathBuf::from("/tmp/pq-ledger/suite_a.jsonl")
        );
        unsafe { std::env::set_var("PQUEUE_LEDGER_DIR", "docs/perf/evidence") };
        assert_eq!(
            ledger_path("/repo/crates/x", "suite_a"),
            std::path::PathBuf::from("/repo/crates/x/../../docs/perf/evidence/suite_a.jsonl")
        );
        unsafe { std::env::remove_var("PQUEUE_LEDGER_DIR") };
        assert_eq!(
            ledger_path("/repo/crates/x", "suite_a"),
            std::path::PathBuf::from("/repo/crates/x/../../target/pqueue-ledger/suite_a.jsonl")
        );
    }

    #[test]
    fn missing_evidence_reports_the_gap() {
        let mut s = LedgerSummary::default();
        s.evidence_ids.extend(["E2".to_string(), "E3".to_string()]);
        let missing = missing_evidence(&s, &["E0", "E1", "E2", "E3"].map(String::from));
        assert_eq!(missing, vec!["E0".to_string(), "E1".to_string()]);
    }
}
