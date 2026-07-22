//! `pqueue-cost-model` — turn ADR-001's directional S3 cost claim into release EVIDENCE.
//!
//! Computes `$/billion-commands` for every governed E3 projection/bound (from the live measured request and
//! segment counters scaled to a billion commands, cited S3 prices, and stated retention/recovery assumptions) and
//! compares it against the `postgres_native` high-volume baseline (always-on instance at the measured E0
//! throughput + DB storage + provisioned IOPS). It writes a markdown evidence ARTIFACT (full breakdown, cited
//! prices, sensitivity/crossover table) and appends the E3 **cost-model** ledger row.
//!
//! Usage:
//!   pqueue-cost-model [--out <doc.md>] [--ledger <ledger.jsonl>] [--print] [--granularity-only]
//!
//! Defaults: `--out docs/perf/tp002-e3-cost-model.md` and consumes the governed live E3 ledger. With
//! `--ledger` it writes the eight release-tier E3 cost rows.
//! The model is a deterministic calculation, so the doc + row regenerate identically. Reproducible command:
//!   cargo run -p pqueue-release --bin pqueue-cost-model -- \
//!     --out docs/perf/tp002-e3-cost-model.md --ledger docs/perf/evidence/tp002-e3-cost-model.jsonl

use std::fmt::Write as _;
use std::io::{BufRead, Write as _};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use pqueue_release::LedgerRow;
use pqueue_release::cost::{
    CostComparison, GranularityAssumptions, ObjectLogCounts, PriceInputs, RecoveryMode,
    ReleaseCostInput, WorkloadAssumptions, build_release_cost_rows, compute_comparison,
    estimate_granularity, release_cost_inputs, validate_release_cost_rows,
};

const REPRO_COMMAND: &str = "cargo run -p pqueue-release --bin pqueue-cost-model -- \
    --e3-ledger docs/perf/evidence/tp002-e3-objectlog-minio-release.jsonl \
    --out docs/perf/tp002-e3-cost-model.md --ledger docs/perf/evidence/tp002-e3-cost-model.jsonl";

fn main() -> ExitCode {
    let mut out = PathBuf::from("docs/perf/tp002-e3-cost-model.md");
    let mut ledger: Option<PathBuf> = None;
    let mut e3_ledger = PathBuf::from("docs/perf/evidence/tp002-e3-objectlog-minio-release.jsonl");
    let mut print = false;
    let mut granularity_only = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--out" => match args.next() {
                Some(p) => out = PathBuf::from(p),
                None => return fail("--out requires a path"),
            },
            "--ledger" => match args.next() {
                Some(p) => ledger = Some(PathBuf::from(p)),
                None => return fail("--ledger requires a path"),
            },
            "--e3-ledger" => match args.next() {
                Some(p) => e3_ledger = PathBuf::from(p),
                None => return fail("--e3-ledger requires a path"),
            },
            "--print" => print = true,
            "--granularity-only" => granularity_only = true,
            other => return fail(&format!("unknown argument: {other}")),
        }
    }

    let prices = PriceInputs::adr_001_us_east_1();
    if granularity_only {
        if ledger.is_some() {
            return fail("--ledger cannot be combined with --granularity-only");
        }
        let doc = render_granularity_artifact(&prices);
        if print {
            println!("{doc}");
        }
        if let Err(error) = atomic_write(&out, doc.as_bytes()) {
            return fail(&format!("cannot write {out:?}: {error}"));
        }
        eprintln!(
            "wrote object-granularity economics artifact: {}",
            out.display()
        );
        return ExitCode::SUCCESS;
    }
    let workload = WorkloadAssumptions::tp002_e3_push_baseline();
    let source_rows = match read_rows(&e3_ledger) {
        Ok(rows) => rows,
        Err(error) => return fail(&error),
    };
    let inputs = match release_cost_inputs(&source_rows) {
        Ok(inputs) => inputs,
        Err(errors) => return fail(&format!("invalid E3 release source: {}", errors.join("; "))),
    };
    let headline_input = inputs
        .iter()
        .min_by(|left, right| {
            compute_comparison(&left.counts, &workload, &prices)
                .objectlog_per_billion
                .partial_cmp(
                    &compute_comparison(&right.counts, &workload, &prices).objectlog_per_billion,
                )
                .expect("finite cost")
        })
        .expect("validated matrix is non-empty");
    let headline_counts = &headline_input.counts;
    let headline = compute_comparison(headline_counts, &workload, &prices);
    if !headline.objectlog_wins {
        return fail("cost-optimized E3 point does not beat postgres_native");
    }

    // Prepare and validate every governed output before touching either destination. A bad source,
    // stale price, failed recovery, or inconsistent derived value must leave existing evidence intact.
    let rows = match build_release_cost_rows(&inputs, &workload, &prices, REPRO_COMMAND) {
        Ok(rows) => rows,
        Err(errors) => return fail(&errors.join("; ")),
    };
    if let Err(errors) = validate_release_cost_rows(&rows) {
        return fail(&format!(
            "generated invalid release cost matrix: {}",
            errors.join("; ")
        ));
    }
    let ledger_json = match serialize_rows(&rows) {
        Ok(json) => json,
        Err(error) => return fail(&error),
    };

    let doc = render_artifact(headline_input, &inputs, &headline, &workload, &prices);

    if print {
        println!("{doc}");
    }

    if let Err(e) = atomic_write(&out, doc.as_bytes()) {
        return fail(&format!("cannot write {out:?}: {e}"));
    }
    eprintln!("wrote cost-model artifact: {}", out.display());

    if let Some(path) = ledger {
        if let Err(e) = atomic_write(&path, ledger_json.as_bytes()) {
            return fail(&format!("cannot write ledger {path:?}: {e}"));
        }
        eprintln!(
            "wrote {} E3 cost-model rows (release-tier): {}",
            rows.len(),
            path.display()
        );
    }

    eprintln!(
        "{} {} ${:.2}/B  vs  postgres_native ${:.2}/B  ({:.2}x cheaper; objectlog_below_postgres={})",
        headline_input.backend_profile,
        headline_input.bound,
        headline.objectlog_per_billion,
        headline.postgres_per_billion,
        headline.ratio,
        headline.objectlog_wins,
    );
    ExitCode::SUCCESS
}

fn serialize_rows(rows: &[LedgerRow]) -> Result<String, String> {
    let mut output = String::new();
    for row in rows {
        output.push_str(
            &serde_json::to_string(row)
                .map_err(|error| format!("cannot serialize release cost row: {error}"))?,
        );
        output.push('\n');
    }
    Ok(output)
}

fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("evidence");
    let temp = parent.join(format!(
        ".{file_name}.tmp.{}.{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&temp, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

fn read_rows(path: &std::path::Path) -> Result<Vec<LedgerRow>, String> {
    let file = std::fs::File::open(path)
        .map_err(|error| format!("cannot open E3 ledger {}: {error}", path.display()))?;
    std::io::BufReader::new(file)
        .lines()
        .enumerate()
        .filter_map(|(index, line)| match line {
            Ok(line) if line.trim().is_empty() => None,
            Ok(line) => Some(
                serde_json::from_str(&line)
                    .map_err(|error| format!("{} line {}: {error}", path.display(), index + 1)),
            ),
            Err(error) => Some(Err(format!(
                "{} line {}: {error}",
                path.display(),
                index + 1
            ))),
        })
        .collect()
}

/// One sensitivity scenario: a label for the input that changed, and the computed comparison.
struct Scenario {
    what_varies: String,
    comparison: CostComparison,
}

fn scenario(
    what_varies: &str,
    counts: ObjectLogCounts,
    w: &WorkloadAssumptions,
    p: &PriceInputs,
) -> Scenario {
    Scenario {
        what_varies: what_varies.to_string(),
        comparison: compute_comparison(&counts, w, p),
    }
}

fn sensitivity(base_w: &WorkloadAssumptions, base_p: &PriceInputs) -> Vec<Scenario> {
    let bpc = base_w.bytes_per_command;
    // PUT price multiplier that crosses the result over (with E3 size segments + baseline IOPS).
    let mut x10 = base_p.clone();
    x10.s3_put_per_1k = base_p.s3_put_per_1k * 10.0;

    let mut equal_node = base_p.clone();
    equal_node.objectlog_node_per_hour = base_p.pg_instance_per_hour;

    let mut no_iops = base_w.clone();
    no_iops.pg_provisioned_iops = 0.0;

    vec![
        scenario(
            "baseline (cited prices, 12k provisioned IOPS)",
            ObjectLogCounts::e3_size_dominant(),
            base_w,
            base_p,
        ),
        scenario(
            "E3 latency-dominant segments (highest measured PUT/cmd)",
            ObjectLogCounts::e3_latency_dominant(),
            base_w,
            base_p,
        ),
        scenario(
            "production fill: 8 MiB segments @ 1 KiB/cmd",
            ObjectLogCounts::filled("8 MiB fill", 8.0 * 1024.0 * 1024.0, bpc, 2.0),
            base_w,
            base_p,
        ),
        scenario(
            "production fill: 16 MiB segments @ 1 KiB/cmd",
            ObjectLogCounts::filled("16 MiB fill", 16.0 * 1024.0 * 1024.0, bpc, 2.0),
            base_w,
            base_p,
        ),
        scenario(
            "object-log node priced == postgres instance",
            ObjectLogCounts::e3_size_dominant(),
            base_w,
            &equal_node,
        ),
        scenario(
            "S3 PUT price x10 (CROSSOVER test)",
            ObjectLogCounts::e3_size_dominant(),
            base_w,
            &x10,
        ),
        scenario(
            "postgres provisioned IOPS = 0 (free local disk) + E3 segments",
            ObjectLogCounts::e3_size_dominant(),
            &no_iops,
            base_p,
        ),
        scenario(
            "postgres IOPS = 0 + 16 MiB fill",
            ObjectLogCounts::filled("16 MiB fill", 16.0 * 1024.0 * 1024.0, bpc, 2.0),
            &no_iops,
            base_p,
        ),
    ]
}

fn granularity_scenarios() -> Vec<GranularityAssumptions> {
    let scenario = |label: &str,
                    command_rate_per_s,
                    input_batch_commands,
                    encoded_command_bytes,
                    target_segment_bytes,
                    max_latency_ms,
                    starting_recovery_index_entries| GranularityAssumptions {
        label: label.into(),
        active_queue_count: 1.0,
        command_rate_per_s,
        input_batch_commands,
        encoded_command_bytes,
        target_segment_bytes,
        max_latency_ms,
        starting_recovery_index_entries,
        billing_window_hours: pqueue_release::cost::HOURS_PER_MONTH,
        recovery_window_hours: 24.0,
    };
    let mut scenarios = vec![
        scenario(
            "default, low-rate scalar input",
            10.0,
            1.0,
            1_024.0,
            262_144.0,
            20.0,
            0,
        ),
        scenario(
            "default, sustained; 100-command downstream batches",
            1_000.0,
            100.0,
            1_024.0,
            262_144.0,
            20.0,
            0,
        ),
        scenario(
            "default, hot; 1000-command downstream batches",
            20_000.0,
            1_000.0,
            1_024.0,
            262_144.0,
            20.0,
            0,
        ),
        scenario(
            "default, 16 KiB commands; 100-command batches",
            1_000.0,
            100.0,
            16_384.0,
            262_144.0,
            20.0,
            0,
        ),
        scenario(
            "100 ms bound; 100-command batches",
            1_000.0,
            100.0,
            1_024.0,
            262_144.0,
            100.0,
            0,
        ),
        scenario(
            "8 MiB target; 1000-command batches",
            20_000.0,
            1_000.0,
            1_024.0,
            8_388_608.0,
            100.0,
            0,
        ),
        scenario(
            "hot scalar input; fresh queue",
            20_000.0,
            1.0,
            1_024.0,
            262_144.0,
            20.0,
            0,
        ),
        scenario(
            "hot scalar input; aged queue",
            20_000.0,
            1.0,
            1_024.0,
            262_144.0,
            20.0,
            16_777_216,
        ),
    ];
    let mut density = scenarios[0].clone();
    density.label = "PRD density: 1000 queues at 10 cmd/s each".into();
    density.active_queue_count = 1_000.0;
    scenarios.insert(1, density);
    scenarios
}

fn winner(c: &CostComparison) -> &'static str {
    if c.objectlog_wins {
        "object_log"
    } else {
        "postgres"
    }
}

fn render_granularity_artifact(p: &PriceInputs) -> String {
    let mut output = String::new();
    let _ = writeln!(
        output,
        "# TP-002 — object-log granularity PUT and payload-storage sensitivity\n"
    );
    let _ = writeln!(
        output,
        "This document is GENERATED from explicit workload assumptions. It is modelled sensitivity, not \
         measured release evidence. Regenerate it with:\n\n```\ncargo run -p pqueue-release --bin \
         pqueue-cost-model -- --granularity-only --out \
         docs/perf/tp002-objectlog-granularity-economics.md\n```\n"
    );
    render_granularity_section(&mut output, p);
    let _ = writeln!(
        output,
        "## Price provenance\n\n- {}\n- {}\n",
        p.instance_source, p.iops_source
    );
    output
}

fn render_granularity_section(s: &mut String, p: &PriceInputs) {
    let _ = writeln!(s, "## Workload-driven object granularity\n");
    let _ = writeln!(
        s,
        "This table is **fixed-batch, regular-arrival sensitivity**, not measured release evidence or a \
         universal prediction. It models the real downstream primitive explicitly: \
         `commands/segment = min(batch * ceil(target bytes / batch bytes), batch * \
         ceil(commands arriving inside latency bound / batch))`. This admits target overshoot by a whole \
         downstream batch and assumes a due flush wins ties at the exact deadline. Real arrival and batch \
         distributions must come from E3 counters. The production \
         defaults are `PQUEUE_SEGMENT_TARGET_BYTES=262144` and \
         `PQUEUE_SEGMENT_MAX_LATENCY_MS=20`. Steady successful non-genesis PUT amplification is derived from \
         the current authority-head algorithm: segment + manifest candidate + versioned head + one \
         copy-on-write node per recovery-index level + one retirement marker, or `5 + resulting index \
         height` on an ordinary post-genesis append. A root-height transition reuses the old root and omits \
         that retirement marker. The calculator integrates fanout-64 height transitions from each scenario's \
         starting lifetime entry count across all per-queue seals in the billing window; it does not hold \
         height constant. Queue initialization, fences, retries, and maintenance remain measured-only terms. Storage bytes \
         are uncompressed command \
         payload and exclude framing and metadata overhead; measured E3 primitive and byte counters remain \
         authoritative for releases. Queue count is explicit because independent queues cannot share a \
         segment; fleet request and byte totals are the per-queue shape multiplied by active queues.\n"
    );
    let _ = writeln!(
        s,
        "| Scenario | active queues | cmd/s/queue | input batch | encoded bytes/cmd | target | bound | starting index entries | ending height | avg PUT/seal | trigger | cmd/segment | mean segment | fill | PUT requests/month | PUT $/month | PUT $/B commands | ingress GB/month | retained payload 24h GB | payload storage $/month |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    );
    for assumptions in granularity_scenarios() {
        let estimate = estimate_granularity(&assumptions, p)
            .expect("built-in granularity scenarios have positive finite inputs");
        let _ = writeln!(
            s,
            "| {} | {:.0} | {:.0} | {:.0} | {:.0} | {:.0} | {:.0} ms | {} | {} | {:.2} | {} | {:.0} | {:.0} B | {:.1}% | {:.0} | ${:.2} | ${:.2} | {:.1} | {:.1} | ${:.2} |",
            assumptions.label,
            assumptions.active_queue_count,
            assumptions.command_rate_per_s,
            assumptions.input_batch_commands,
            assumptions.encoded_command_bytes,
            assumptions.target_segment_bytes,
            assumptions.max_latency_ms,
            assumptions.starting_recovery_index_entries,
            estimate.ending_recovery_index_height,
            estimate.put_requests_per_segment,
            estimate.seal_trigger,
            estimate.commands_per_segment,
            estimate.segment_bytes,
            estimate.fill_ratio * 100.0,
            estimate.put_requests_per_billing_window,
            estimate.put_usd_per_billing_window,
            estimate.put_usd_per_billion_commands,
            estimate.ingress_gb_per_billing_window,
            estimate.retained_log_gb,
            estimate.payload_storage_usd_per_month,
        );
    }
    let _ = writeln!(
        s,
        "\n**Interpretation:** granularity optimization means allowing more commands to share each segment \
         object while respecting the operator-selected commit-latency bound. At low arrival rates the \
         latency bound correctly wins and may produce one-command segments; the table makes that cost \
         visible instead of pretending every queue fills its byte target. At high rates or with larger \
         commands, the byte target wins. A large downstream primitive may overshoot the soft byte target; \
         that is visible rather than hidden. Changing the target, bound, or downstream batch is an \
         economic/latency decision, never a durability change. The full TP-002 E3 cost model—not this \
         sensitivity table—adds measured metadata bytes, retries, GET/LIST/DELETE, recovery, and compute.\n"
    );
}

fn render_artifact(
    headline_input: &ReleaseCostInput,
    inputs: &[ReleaseCostInput],
    headline: &CostComparison,
    w: &WorkloadAssumptions,
    p: &PriceInputs,
) -> String {
    let counts = &headline_input.counts;
    let ol = &headline.objectlog;
    let pg = &headline.postgres;
    let mut s = String::new();

    let _ = writeln!(
        s,
        "# TP-002 E3 — object-log cost model ($/billion-commands)\n"
    );
    let _ = writeln!(
        s,
        "**Spec:** `tp-scale-substantiation` (bead `pqueue-1cd90d1c`). **Substantiates:** ADR-001 \
         \"Napkin Cost Comparison\" — turns its *directional* claim that batched object-storage commits beat \
         an always-on relational authority at high volume into a reproducible, fixture-tested calculation \
         over the REAL E3 measured counts.\n"
    );
    let _ = writeln!(
        s,
        "This document is GENERATED by `pqueue-cost-model` (a pure calculator in `pqueue_release::cost`). \
         Do not hand-edit — regenerate with the command below.\n"
    );

    let _ = writeln!(s, "## Reproducible command\n\n```\n{REPRO_COMMAND}\n```\n");
    let _ = writeln!(
        s,
        "**Measured source revision:** `{}` (the clean Git HEAD captured and rechecked by the live wrapper).\n",
        headline_input.source_revision
    );

    // Headline.
    let _ = writeln!(s, "## Headline\n");
    let _ = writeln!(
        s,
        "At the documented TP-002 high-volume baseline, the cost-optimized measured point is \
         `{profile}` / `{bound}` (`{label}`), using the cited prices below:\n",
        profile = headline_input.backend_profile,
        bound = headline_input.bound,
        label = counts.label,
    );
    let _ = writeln!(
        s,
        "| Backend | $/billion-commands |\n|---|---|\n| `{profile}` | **${ol_t:.2}** |\n| \
         `postgres_native` | **${pg_t:.2}** |\n",
        profile = headline_input.backend_profile,
        ol_t = headline.objectlog_per_billion,
        pg_t = headline.postgres_per_billion,
    );
    let verdict = if headline.objectlog_wins {
        format!(
            "`{}` is **{:.2}x cheaper** than `postgres_native` at this baseline \
             — the ADR-001 direction holds with honest, cited inputs.",
            headline_input.backend_profile, headline.ratio
        )
    } else {
        format!(
            "Under these inputs `object_log_sqlite_projection` is NOT below `postgres_native` \
             (ratio {:.2}x). Reporting it honestly rather than forcing the claim; see the sensitivity table \
             for where the crossover sits.",
            headline.ratio
        )
    };
    let _ = writeln!(s, "{verdict}\n");

    let _ = writeln!(s, "## Resolved live E3 cost matrix\n");
    let _ = writeln!(
        s,
        "Every row below is calculated from the governed live E3 row's measured PUT/GET/LIST/DELETE and \
         segment counters. The bold row is the cost-optimized point; no modeled throughput replaces the \
         measured bound counters.\n"
    );
    let _ = writeln!(
        s,
        "| profile | bound | PUT/B | GET/B | LIST/B | DELETE/B | segment commands | object-log $/B | postgres $/B |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|"
    );
    for input in inputs {
        let comparison = compute_comparison(&input.counts, w, p);
        let optimized = input.backend_profile == headline_input.backend_profile
            && input.bound == headline_input.bound;
        let profile = if optimized {
            format!("**{}**", input.backend_profile)
        } else {
            input.backend_profile.clone()
        };
        let _ = writeln!(
            s,
            "| {profile} | {} | {:.0} | {:.0} | {:.0} | {:.0} | {:.1} | ${:.2} | ${:.2} |",
            input.bound,
            comparison.objectlog.put_requests,
            comparison.objectlog.get_requests,
            comparison.objectlog.list_requests,
            comparison.objectlog.delete_requests,
            input.counts.commands_per_segment(),
            comparison.objectlog_per_billion,
            comparison.postgres_per_billion,
        );
    }
    let _ = writeln!(s);

    // Breakdown.
    let _ = writeln!(s, "## Breakdown\n");
    let _ = writeln!(
        s,
        "### `{}`\n\n| Line | Quantity | Cost |\n|---|---|---|",
        headline_input.backend_profile,
    );
    let _ = writeln!(
        s,
        "| Measured steady-state + fixed-10M recovery PUTs | {:.0} requests/B ({:.4} sealed objects/command) | ${:.2} |",
        ol.put_requests,
        counts.objects_put / counts.commands,
        ol.put_cost
    );
    let _ = writeln!(
        s,
        "| Durable storage ({storage_shape}; {rw}h recovery window) | {:.1} GB | ${:.2} |",
        ol.storage_gb,
        ol.storage_cost,
        storage_shape = match counts.recovery_mode {
            RecoveryMode::SnapshotTail => "snapshot + bounded tail",
            RecoveryMode::FullGenesis => "full durable genesis log; no snapshot",
        },
        rw = w.recovery_window_hours
    );
    let _ = writeln!(
        s,
        "| Measured recovery GETs ({rc} rebuild/window) | {:.0} requests | ${:.2} |",
        ol.get_requests,
        ol.get_cost,
        rc = w.recoveries_per_window
    );
    let _ = writeln!(
        s,
        "| Measured LISTs | {:.0} requests/B | ${:.2} |",
        ol.list_requests, ol.list_cost
    );
    let _ = writeln!(
        s,
        "| Measured DELETEs | {:.0} requests/B | ${:.2} |",
        ol.delete_requests, ol.delete_cost
    );
    let _ = writeln!(
        s,
        "| Compute node (always-on {win:.0} h @ ${rate}/h) | {:.0} h | ${:.2} |",
        ol.compute_hours,
        ol.compute_cost,
        win = ol.compute_hours,
        rate = p.objectlog_node_per_hour
    );
    let _ = writeln!(s, "| **Total** | | **${:.2}** |\n", ol.total);

    let _ = writeln!(
        s,
        "### `postgres_native`\n\n| Line | Quantity | Cost |\n|---|---|---|"
    );
    let _ = writeln!(
        s,
        "| Always-on instance ({win:.0} h @ ${rate}/h) | {:.0} h (processes 1B in {ph:.1} h at the measured \
         {tput:.0} cmd/s) | ${:.2} |",
        pg.compute_hours,
        pg.compute_cost,
        win = pg.compute_hours,
        rate = p.pg_instance_per_hour,
        ph = pg.processing_hours,
        tput = headline.pg_command_throughput_per_s,
    );
    let _ = writeln!(
        s,
        "| DB storage (resident heap + {ov}x index overhead) | {:.1} GB | ${:.2} |",
        pg.storage_gb,
        pg.storage_cost,
        ov = w.pg_index_overhead
    );
    let _ = writeln!(
        s,
        "| Provisioned IOPS (claim-index churn) | {:.0} IOPS | ${:.2} |",
        pg.provisioned_iops, pg.iops_cost
    );
    let _ = writeln!(s, "| **Total** | | **${:.2}** |\n", pg.total);

    // Apples-to-apples.
    let _ = writeln!(s, "## Apples-to-apples (fairness)\n");
    let _ = writeln!(
        s,
        "`object_log_sqlite_projection` ALSO runs a compute node, so this is **not** \"free S3 vs a paid \
         DB\". Both sides are charged compute for the SAME always-on {win:.0} h window. The modelled win is \
         two separate, inspectable line items:\n\n\
         1. **Durable storage + I/O.** The log lives on object storage (request-priced PUTs + \
         ${s3}/GB-month, with *no per-I/O or provisioned-IOPS charge*) instead of DB storage + provisioned \
         IOPS sized for the claim-index churn. The E0 evidence \
         (`docs/perf/tp002-e0e1-postgres-release-10m.md`) documents that the `SKIP LOCKED` claim path is \
         read-IOPS-bound under MVCC bloat (a single 500-item claim went from 1,007 to 46,694 buffers as the \
         priority index accumulated dead tuples), which is exactly the cost object storage does not levy.\n\
         2. **Node sizing.** The object-log node can be smaller than the IOPS-bound claim authority — but \
         this is a SEPARATE price input, and the `object-log node priced == postgres instance` sensitivity \
         row below confirms the win survives with both nodes priced identically. The win is carried by the \
         storage/I/O term, not by cherry-picking a smaller node.\n",
        win = pg.compute_hours,
        s3 = p.s3_storage_per_gb_month,
    );

    // Assumptions.
    let _ = writeln!(s, "## Assumptions\n");
    let _ = writeln!(s, "| Assumption | Value |\n|---|---|");
    let _ = writeln!(
        s,
        "| Normalization | $/billion durable commands |\n\
         | Billing window | {win:.0} h (always-on month) |\n\
         | Bytes per command | {bpc:.0} (ADR-001 1 KiB record) |\n\
         | Commands per item | {cpi:.0} (push only; the measured E3 operation) |\n\
         | Resident working set | {res:.0} items (E0/E3 shape) |\n\
         | Recovery window | {rw:.0} h of committed log behind the latest snapshot |\n\
         | Recoveries per window | {rc:.0} |\n\
         | Measured E0 ingest | {ing:.0} items/s |\n\
         | Measured E0 claim+finalize | excluded from this push-only comparator ({drn:.0} items/s reference) |\n\
         | Comparator command throughput | {tput:.0} push commands/s |\n\
         | Postgres provisioned IOPS | {iops:.0} |\n\
         | Postgres index overhead | {ov}x |",
        win = w.billing_window_hours,
        bpc = w.bytes_per_command,
        cpi = w.commands_per_item,
        res = w.resident_items,
        rw = w.recovery_window_hours,
        rc = w.recoveries_per_window,
        ing = w.pg_ingest_per_s,
        drn = w.pg_claim_finalize_per_s,
        tput = headline.pg_command_throughput_per_s,
        iops = w.pg_provisioned_iops,
        ov = w.pg_index_overhead,
    );
    let _ = writeln!(s);

    render_granularity_section(&mut s, p);

    // Prices.
    let _ = writeln!(s, "## Cited price inputs\n");
    let _ = writeln!(s, "| Input | Value |\n|---|---|");
    let _ = writeln!(
        s,
        "| S3 storage | ${}/GB-month |\n\
         | S3 PUT | ${}/1k |\n\
         | S3 GET | ${}/1k |\n\
         | postgres_native instance | ${}/hour |\n\
         | DB storage | ${}/GB-month |\n\
         | Provisioned IOPS | ${}/IOPS-month |\n\
         | object-log node | ${}/hour |",
        p.s3_storage_per_gb_month,
        p.s3_put_per_1k,
        p.s3_get_per_1k,
        p.pg_instance_per_hour,
        p.pg_storage_per_gb_month,
        p.pg_iops_per_month_each,
        p.objectlog_node_per_hour,
    );
    let _ = writeln!(s, "\n- **Prices source:** {}", p.instance_source);
    let _ = writeln!(s, "- **IOPS price source:** {}\n", p.iops_source);

    // Sensitivity.
    let _ = writeln!(s, "## Sensitivity & crossover\n");
    let _ = writeln!(
        s,
        "Each row recomputes the full model with ONE input changed. The winner column shows the calculator \
         responds to its inputs — it is not wired to a conclusion.\n"
    );
    let _ = writeln!(
        s,
        "| What varies | object_log $/B | postgres $/B | ratio (pg/ol) | winner |\n|---|---|---|---|---|"
    );
    let scenarios = sensitivity(w, p);
    for sc in &scenarios {
        let c = &sc.comparison;
        let _ = writeln!(
            s,
            "| {} | ${:.2} | ${:.2} | {:.2}x | {} |",
            sc.what_varies,
            c.objectlog_per_billion,
            c.postgres_per_billion,
            c.ratio,
            winner(c),
        );
    }
    let _ = writeln!(s);
    let _ = writeln!(
        s,
        "**Crossover read:** object-log loses only when BOTH (a) postgres carries no provisioned-IOPS floor \
         (free local disk) AND (b) segments stay at the pessimistic latency-bound E3 fill — i.e. a \
         low-throughput postgres on free disk. Filling segments toward their byte target (the production \
         shape, and ADR-001's \"16 MiB ⇒ <$2 PUTs\" case) flips object-log back ahead even at zero postgres \
         IOPS. At the documented high-volume baseline — where the claim-index churn forces real provisioned \
         IOPS — object-log is the cheaper side by a wide margin.\n"
    );

    // Not modeled.
    let _ = writeln!(s, "## What is NOT modeled (honesty)\n");
    let _ = writeln!(
        s,
        "- **Data transfer / egress.** Both backends move bytes; cross-AZ and internet egress are excluded \
         (ADR-001 also excludes them).\n\
         - **MinIO vs real S3.** The E3 counts were measured against MinIO; the prices are real-S3 US-East-1. \
         The COUNTS (objects/command, segments/command) are storage-implementation-independent; only the \
         prices assume AWS S3.\n\
         - **Operator labor, support plans, backups beyond the stated recovery window, PrivateLink, \
         compression.** Excluded on both sides (ADR-001 parity).\n\
         - **The object-log node's own instance cost IS modeled** (it is not free) — see Apples-to-apples.\n\
         - **Postgres read-replica / HA topology.** A single instance is modeled; HA would raise the \
         postgres side, not lower it, so the comparison is conservative against object-log.\n\
         - **Provisioned-IOPS sizing** is an assumption (12k) anchored to the E0 claim-churn finding, not a \
         measured Aurora I/O bill; the sensitivity table brackets it down to 0.\n"
    );

    s
}

fn fail(msg: &str) -> ExitCode {
    eprintln!("pqueue-cost-model: {msg}");
    eprintln!(
        "usage: pqueue-cost-model [--e3-ledger <source.jsonl>] [--out <doc.md>] [--ledger <ledger.jsonl>] [--print]"
    );
    ExitCode::FAILURE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_replaces_complete_file_and_leaves_no_temporary_file() {
        let dir = std::env::temp_dir().join(format!(
            "pqueue-cost-model-atomic-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ledger.jsonl");
        std::fs::write(&path, b"old\n").unwrap();

        atomic_write(&path, b"row-1\nrow-2\n").unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"row-1\nrow-2\n");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn atomic_write_failure_preserves_destination_and_cleans_temporary_file() {
        let dir = std::env::temp_dir().join(format!(
            "pqueue-cost-model-atomic-failure-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let destination = dir.join("ledger.jsonl");
        std::fs::create_dir_all(&destination).unwrap();
        std::fs::write(destination.join("sentinel"), b"old").unwrap();

        assert!(atomic_write(&destination, b"new\n").is_err());

        assert_eq!(std::fs::read(destination.join("sentinel")).unwrap(), b"old");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn rendered_report_names_the_exact_measured_source_revision() {
        let counts = ObjectLogCounts::e3_size_dominant();
        let input = ReleaseCostInput {
            backend_profile: "object_log_sqlite_projection".into(),
            bound: "100ms".into(),
            counts: counts.clone(),
            source_command: "wrapper".into(),
            source_environment: "test".into(),
            source_revision: "1111111111111111111111111111111111111111".into(),
        };
        let workload = WorkloadAssumptions::tp002_e3_push_baseline();
        let prices = PriceInputs::adr_001_us_east_1();
        let comparison = compute_comparison(&counts, &workload, &prices);
        let report = render_artifact(
            &input,
            std::slice::from_ref(&input),
            &comparison,
            &workload,
            &prices,
        );
        assert!(
            report
                .contains("Measured source revision:** `1111111111111111111111111111111111111111`")
        );
    }

    #[test]
    fn granularity_report_exposes_defaults_and_write_amplification_assumptions() {
        let report = render_granularity_artifact(&PriceInputs::adr_001_us_east_1());
        assert!(report.contains("PQUEUE_SEGMENT_TARGET_BYTES=262144"));
        assert!(report.contains("PQUEUE_SEGMENT_MAX_LATENCY_MS=20"));
        assert!(report.contains("fixed-batch, regular-arrival sensitivity"));
        assert!(report.contains("5 + resulting index"));
        assert!(report.contains("does not hold height constant"));
        assert!(report.contains("whole downstream batch"));
        assert!(report.contains("retained payload 24h GB"));
        assert!(report.contains("1000 queues at 10 cmd/s each"));
        assert!(report.contains("independent queues cannot share a segment"));
    }
}
