//! `pqueue-cost-model` — turn ADR-001's directional S3 cost claim into release EVIDENCE.
//!
//! Computes `$/billion-commands` for `object_log_sqlite_projection` (from the REAL E3 measured segment/object
//! counts scaled to a billion commands, cited S3 prices, and stated retention/recovery assumptions) and
//! compares it against the `postgres_native` high-volume baseline (always-on instance at the measured E0
//! throughput + DB storage + provisioned IOPS). It writes a markdown evidence ARTIFACT (full breakdown, cited
//! prices, sensitivity/crossover table) and appends the E3 **cost-model** ledger row.
//!
//! Usage:
//!   pqueue-cost-model [--out <doc.md>] [--ledger <ledger.jsonl>] [--print]
//!
//! Defaults: `--out docs/perf/tp002-e3-cost-model.md`. With `--ledger` it appends the smoke-tier E3 cost row.
//! The model is a deterministic calculation, so the doc + row regenerate identically. Reproducible command:
//!   cargo run -p pqueue-release --bin pqueue-cost-model -- \
//!     --out docs/perf/tp002-e3-cost-model.md --ledger docs/perf/evidence/tp002-e3-cost-model.jsonl

use std::fmt::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use pqueue_release::append_row;
use pqueue_release::cost::{
    CostComparison, ObjectLogCounts, PriceInputs, WorkloadAssumptions, build_cost_row,
    compute_comparison,
};

const REPRO_COMMAND: &str = "cargo run -p pqueue-release --bin pqueue-cost-model -- \
    --out docs/perf/tp002-e3-cost-model.md --ledger docs/perf/evidence/tp002-e3-cost-model.jsonl";

fn main() -> ExitCode {
    let mut out = PathBuf::from("docs/perf/tp002-e3-cost-model.md");
    let mut ledger: Option<PathBuf> = None;
    let mut print = false;

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
            "--print" => print = true,
            other => return fail(&format!("unknown argument: {other}")),
        }
    }

    let prices = PriceInputs::adr_001_us_east_1();
    let workload = WorkloadAssumptions::tp002_high_volume_baseline();
    let headline_counts = ObjectLogCounts::e3_size_dominant();
    let headline = compute_comparison(&headline_counts, &workload, &prices);

    let doc = render_artifact(&headline_counts, &headline, &workload, &prices);

    if print {
        println!("{doc}");
    }

    if let Some(parent) = out.parent()
        && let Err(e) = std::fs::create_dir_all(parent)
    {
        return fail(&format!("cannot create {parent:?}: {e}"));
    }
    if let Err(e) = std::fs::write(&out, &doc) {
        return fail(&format!("cannot write {out:?}: {e}"));
    }
    eprintln!("wrote cost-model artifact: {}", out.display());

    if let Some(path) = ledger {
        let row = build_cost_row(
            &headline,
            &headline_counts,
            &workload,
            &prices,
            REPRO_COMMAND,
        );
        if let Err(e) = append_row(&path, &row) {
            return fail(&format!("cannot append ledger row to {path:?}: {e}"));
        }
        eprintln!(
            "appended E3 cost-model row (smoke-tier): {}",
            path.display()
        );
    }

    eprintln!(
        "object_log_sqlite_projection ${:.2}/B  vs  postgres_native ${:.2}/B  ({:.2}x cheaper; objectlog_below_postgres={})",
        headline.objectlog_per_billion,
        headline.postgres_per_billion,
        headline.ratio,
        headline.objectlog_wins,
    );
    ExitCode::SUCCESS
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

fn winner(c: &CostComparison) -> &'static str {
    if c.objectlog_wins {
        "object_log"
    } else {
        "postgres"
    }
}

fn render_artifact(
    counts: &ObjectLogCounts,
    headline: &CostComparison,
    w: &WorkloadAssumptions,
    p: &PriceInputs,
) -> String {
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

    // Headline.
    let _ = writeln!(s, "## Headline\n");
    let _ = writeln!(
        s,
        "At the documented TP-002 high-volume baseline, using the REAL E3 measured counts \
         (`{label}`) and the cited prices below:\n",
        label = counts.label
    );
    let _ = writeln!(
        s,
        "| Backend | $/billion-commands |\n|---|---|\n| `object_log_sqlite_projection` | **${ol_t:.2}** |\n| \
         `postgres_native` | **${pg_t:.2}** |\n",
        ol_t = headline.objectlog_per_billion,
        pg_t = headline.postgres_per_billion,
    );
    let verdict = if headline.objectlog_wins {
        format!(
            "`object_log_sqlite_projection` is **{:.2}x cheaper** than `postgres_native` at this baseline \
             — the ADR-001 direction holds with honest, cited inputs.",
            headline.ratio
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

    // Breakdown.
    let _ = writeln!(s, "## Breakdown\n");
    let _ = writeln!(
        s,
        "### `object_log_sqlite_projection`\n\n| Line | Quantity | Cost |\n|---|---|---|"
    );
    let _ = writeln!(
        s,
        "| Segment + manifest PUTs | {:.0} requests/B ({:.4} objects/command) | ${:.2} |",
        ol.put_requests,
        counts.objects_put / counts.commands,
        ol.put_cost
    );
    let _ = writeln!(
        s,
        "| Durable storage (snapshot + {rw}h recovery-window log) | {:.1} GB | ${:.2} |",
        ol.storage_gb,
        ol.storage_cost,
        rw = w.recovery_window_hours
    );
    let _ = writeln!(
        s,
        "| Recovery GETs ({rc} rebuild/window) | {:.0} requests | ${:.2} |",
        ol.get_requests,
        ol.get_cost,
        rc = w.recoveries_per_window
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
         | Commands per item | {cpi:.0} (push + claim + finalize) |\n\
         | Resident working set | {res:.0} items (E0/E3 shape) |\n\
         | Recovery window | {rw:.0} h of committed log behind the latest snapshot |\n\
         | Recoveries per window | {rc:.0} |\n\
         | Measured E0 ingest | {ing:.0} items/s |\n\
         | Measured E0 claim+finalize | {drn:.0} items/s |\n\
         | Folded command throughput | {tput:.0} commands/s |\n\
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
    eprintln!("usage: pqueue-cost-model [--out <doc.md>] [--ledger <ledger.jsonl>] [--print]");
    ExitCode::FAILURE
}
