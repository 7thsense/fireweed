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
use std::path::Path;

use serde::{Deserialize, Serialize};

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
pub fn ledger_path(manifest_dir: &str, suite: &str) -> std::path::PathBuf {
    let dir = match std::env::var("PQUEUE_LEDGER_DIR") {
        Ok(d) if !d.trim().is_empty() => std::path::PathBuf::from(d),
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
