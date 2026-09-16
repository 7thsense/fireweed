"""Performance acceptance for the original-row workload and component ladder."""

import math
import re


def qualify(report, campaign_target=10_000):
    result = report.get("result", {})
    checks = []

    def check(name, passed, actual=None, required=None):
        checks.append({"name": name, "passed": bool(passed), "actual": actual, "required": required})

    check("clean_source", report.get("dirty") is False)
    head = report.get("head")
    binary_sha = report.get("binary_sha256")
    check("source_identity", isinstance(head, str) and bool(re.fullmatch(r"[0-9a-f]{40}", head)))
    check("binary_identity", isinstance(binary_sha, str) and bool(re.fullmatch(r"[0-9a-f]{64}", binary_sha)))
    check("successful_correctness_run", report.get("exit_code") == 0)
    check("filesystem_log_and_turso", result.get("cell") == "filesystem--turso")
    check("storage_evidence_present", bool(report.get("filesystem")))
    check("physical_sharding", result.get("physical_shards", 0) >= 2)
    for name in ("filesystem", "projection_filesystem"):
        if name in report:
            mounts = report[name].get("filesystems", [])
            check(name + "_on_disk", bool(mounts) and all(m.get("fstype") not in ("tmpfs", "ramfs", None) for m in mounts))
    check("no_external_io_override", not report.get("diagnostics", {}).get("LD_PRELOAD"))
    check("no_unqualified_diagnostic_override", "diagnostic_provenance" not in report)
    if result.get("schema") == "primitive-capacity/v1":
        check("known_primitive_payload_workload", result.get("payload_workload", "repeated_padding") in ("repeated_padding", "campaign_varied"))
        if result.get("payload_workload") == "campaign_varied":
            size = result.get("payload_bytes", 0)
            initial = result.get("initial_payload_bytes", 0)
            replacement = result.get("payload_replacement_bytes", 0)
            valid_size = isinstance(size, int) and size >= 1024
            check("representative_component_payload_size", valid_size, size, ">=1024 nominal bytes")
            check("component_payload_bytes", valid_size and isinstance(initial, int)
                  and isinstance(replacement, int)
                  and initial >= result.get("items", 0) * (size - 128)
                  and replacement > initial)
        check("million_resident_rows", result.get("items", 0) >= 1_000_000, result.get("items"), 1_000_000)
        phases = {p["phase"]: p for p in result.get("aggregate_phases", [])}
        for name in ("insert", "enrich_by_key", "schedule_by_id", "claim_and_complete", "purge"):
            phase = phases.get(name, {})
            records = phase.get("records")
            window = phase.get("wall_window_s")
            rate = phase.get("records_per_s", 0)
            complete = isinstance(records, int) and records == result.get("items")
            valid_window = isinstance(window, (int, float)) and math.isfinite(window) and window > 0
            valid_rate = isinstance(rate, (int, float)) and math.isfinite(rate)
            check(name + "_complete_work", complete, records, result.get("items"))
            check(name + "_rate_accounting", complete and valid_window and valid_rate
                  and math.isclose(rate, records / window, rel_tol=1e-9),
                  rate, "records / measured phase window")
            check(name, valid_rate and rate >= 10_000, rate, 10_000)
    elif result.get("schema") == "workflow-capacity/v6":
        check("original_row_workflow", result.get("profile") == "Mutable" and result.get("atomic_original_row_mutation") is True and result.get("dispatch") == "shared-normal-claim")
        check("faults_and_retention", result.get("faults") is True and result.get("includes_purge") is True)
        cycles = result.get("cycles", 0)
        check("at_least_three_cycles", cycles >= 3, cycles, 3)
        count = result.get("items", 0) * cycles
        check("million_complete_workflows", count >= 1_000_000, count, 1_000_000)
        rate = result.get("completed_lifecycles_per_s", 0)
        check("overall_workflows_per_s", isinstance(rate, (int, float)) and math.isfinite(rate) and rate >= 9_500, rate, 9_500)
        shards = result.get("shards", [])
        check("complete_cycle_reports", bool(shards) and len(shards) == result.get("physical_shards") and all(len(s.get("cycles", [])) == cycles for s in shards))
        if shards and cycles >= 3 and all(len(s.get("cycles", [])) == cycles for s in shards):
            if result.get("schema") == "workflow-capacity/v6":
                observation = report.get("projection_wal_observation", {})
                check("wal_sampling_valid", 0 < observation.get("interval_ms", 0) <= 100
                      and report.get("process_wall_s", 0) > 0
                      and observation.get("samples", 0) >= max(2, report.get("process_wall_s", 0) * 8)
                      and observation.get("errors") == [])
                for index, shard in enumerate(shards):
                    sampled_peak = observation.get("peak_bytes", {}).get(f"shard-{index}", 0)
                    endpoints = [c.get("projection_wal_bytes") or 0 for c in shard["cycles"]]
                    peak = max([sampled_peak, *endpoints])
                    # Fixed before measurement: 512 MiB per physical shard.
                    # WAL restart legitimately shrinks the file; flatness is not required.
                    check(f"shard_{index}_wal_within_budget", sampled_peak > 0
                          and all(size > 0 for size in endpoints) and peak <= 512 * 1024 * 1024,
                          peak, "<=512 MiB, sampled and cycle endpoints")
            for index in range(cycles):
                rows = [s["cycles"][index] for s in shards]
                check(f"cycle_{index}_outcomes_reconciled", sum(c.get("items", 0) for c in rows) == result.get("items")
                      and all(c.get("items", 0) > 0 and c.get("pending") == 0 and c.get("leased") == 0
                              and c.get("delivered", -1) >= 0 and c.get("failed", -1) >= 0
                              and c.get("delivered", -1) + c.get("failed", -1) == c.get("items")
                              and c.get("complete") == c.get("delivered")
                              and isinstance(c.get("wall_s"), (int, float))
                              and math.isfinite(c["wall_s"]) and c["wall_s"] > 0 for c in rows))
            # Each shard must sustain its fair share in every cycle, preventing
            # a fast shard or initial burst from hiding starvation or a slowdown.
            for index in range(cycles):
                rows = [s["cycles"][index] for s in shards]
                equivalent_rate = min(c["items"] / c["wall_s"] if isinstance(c.get("wall_s"), (int, float)) and math.isfinite(c["wall_s"]) and c["wall_s"] > 0 else 0 for c in rows) * len(shards)
                check(f"cycle_{index}_slowest_shard_equivalent_rate", equivalent_rate >= 9_500, equivalent_rate, 9_500)
            rss = [max(s["cycles"][i].get("process_rss_kib") or 0 for s in shards) for i in range(cycles - 3, cycles)]
            rss_growth = (max(rss) / min(rss) - 1) if min(rss) > 0 else None
            check("last_three_cycles_rss_stable", rss_growth is not None and rss_growth <= .10, rss_growth, "<=10% range")
            for index, shard in enumerate(shards):
                sizes = [c.get("projection_bytes") or 0 for c in shard["cycles"][-3:]]
                growth = (max(sizes) / min(sizes) - 1) if min(sizes) > 0 else None
                check(f"shard_{index}_projection_stable", growth is not None and growth <= .05, growth, "<=5% range")
                if result.get("schema") == "workflow-capacity/v5":
                    wal_sizes = [c.get("projection_wal_bytes") or 0 for c in shard["cycles"][-3:]]
                    wal_growth = (max(wal_sizes) / min(wal_sizes) - 1) if min(wal_sizes) > 0 else None
                    check(f"shard_{index}_projection_wal_stable", wal_growth is not None and wal_growth <= .10, wal_growth, "<=10% range")
    elif result.get("schema") == "campaign-capacity/v3":
        priority_workload = result.get("priority_workload", "mixed_sequence_stress")
        check("campaign_priority_workload", priority_workload in ("mixed_sequence_stress", "availability_timestamp"))
        storage_mode = result.get("enrichment_storage")
        check("enrichment_storage_declared", storage_mode in ("row_metadata", "payload"))
        check("temporal_retention_model", result.get("lease_ms") == 3_600_000
              and result.get("request_id_retention_ms") == 3_600_000 and result.get("cycle_clock_step_s") == 7200)
        purge_batch = result.get("purge_batch", 0)
        check("bounded_retention_batch", isinstance(purge_batch, int) and 0 < purge_batch <= 8192)
        payload_bytes = result.get("payload_bytes", 0)
        check("representative_payload_size", isinstance(payload_bytes, int) and payload_bytes >= 1024)
        check("campaign_target", campaign_target in (10_000, 12_500), campaign_target, "10000 or 12500")
        check("campaign_shape", result.get("campaigns") == 2 and result.get("scheduled_windows") == 4
              and result.get("stage_limits") == [500, 200, 500] and result.get("faults") is True
              and result.get("includes_purge") is True and result.get("progress_interval_ms") == 1000)
        check("million_resident_campaign_rows", result.get("resident_backlog", 0) >= 1_000_000
              and result.get("resident_backlog") == result.get("items"))
        cycles = result.get("cycles", 0)
        shards = result.get("shards", [])
        shape = (cycles >= 3 and len(shards) == result.get("physical_shards")
                 and all(len(s.get("campaigns", [])) == 2 and all(len(c.get("cycles", [])) == cycles
                         for c in s["campaigns"]) for s in shards))
        check("complete_campaign_reports", shape)
        rate = result.get("completed_lifecycles_per_s", 0)
        check("overall_campaign_recipients_per_s", isinstance(rate, (int, float)) and math.isfinite(rate)
              and rate >= campaign_target, rate, campaign_target)
        observation = report.get("projection_wal_observation", {})
        check("wal_sampling_valid", 0 < observation.get("interval_ms", 0) <= 100
              and report.get("process_wall_s", 0) > 0
              and observation.get("samples", 0) >= max(2, report.get("process_wall_s", 0) * 8)
              and observation.get("errors") == [])
        if shape:
            for cycle in range(cycles):
                rows = [c["cycles"][cycle] for s in shards for c in s["campaigns"]]
                correct = sum(c.get("items", 0) for c in rows) == result.get("items")
                equivalents = []
                for shard_index, shard in enumerate(shards):
                    for campaign_index, campaign in enumerate(shard["campaigns"]):
                        c = campaign["cycles"][cycle]
                        ids = range(shard_index + campaign_index * len(shards), result["items"], 2 * len(shards))
                        n = len(ids)
                        failed = sum(i % 31 == 0 for i in ids)
                        retries = sum(i % 19 == 0 for i in ids)
                        correct = correct and (c.get("items") == n and c.get("verified") == n and c.get("purged") == n
                            and c.get("failed") == failed and c.get("delivered") == n - failed
                            and c.get("retries") == retries and c.get("claims") == 3*n+retries
                            and c.get("handler_rows") == [n,n,n+retries]
                            and len(c.get("max_handler_batch", [])) == 3
                            and all(0 < b <= limit for b,limit in zip(c.get("max_handler_batch", []),[500,200,500])) and c.get("pending") == 0 and c.get("leased") == 0)
                        correct = correct and (
                            c.get("initial_payload_bytes", 0) >= n * max(0, payload_bytes - 128)
                            and c.get("payload_replacements") == (0 if storage_mode == "row_metadata" else 2*n)
                            and (c.get("payload_replacement_bytes") == 0 if storage_mode == "row_metadata"
                                 else c.get("payload_replacement_bytes", 0) >= 2*n*max(0, payload_bytes-128))
                            and c.get("purge_batches", 0) == (n + max(1,purge_batch)-1)//max(1,purge_batch)
                            and c.get("max_purge_batch", 0) == min(n,purge_batch)
                            and c.get("claim_batches", 0) == c.get("mutation_batches", -1)
                            and 0 < c.get("max_claim_batch", 0) <= result.get("batch", 0) <= 1000
                            and c.get("claim_batches", 0) * c.get("max_claim_batch", 0) >= 3*n+retries)
                        delay = c.get("due_to_claim_max_us", float("inf"))
                        check(f"s{shard_index}_c{campaign_index}_cycle{cycle}_due_to_claim",
                              math.isfinite(delay) and 0 <= delay <= 60_000_000, delay, "<=60000000 us")
                        wall = c.get("wall_s", 0)
                        valid_wall = isinstance(wall, (int,float)) and math.isfinite(wall) and wall > 0
                        equivalents.append(n/wall*len(rows) if valid_wall else 0)
                        latency = c.get("progress_p95_s", float("inf"))
                        check(f"s{shard_index}_c{campaign_index}_cycle{cycle}_progress",
                              valid_wall and c.get("progress_reads", 0) >= max(1, wall/2)
                              and math.isfinite(latency) and latency <= 1.0, latency, "p95 <=1s; >=0.5 reads/s")
                check(f"cycle_{cycle}_independent_outcomes", correct)
                check(f"cycle_{cycle}_slowest_campaign_equivalent_rate", min(equivalents) >= campaign_target,
                      min(equivalents), campaign_target)
            rss = [max(c["cycles"][i].get("process_rss_kib") or 0 for s in shards for c in s["campaigns"])
                   for i in range(cycles-3,cycles)]
            check("rss_stable", min(rss)>0 and max(rss)/min(rss)<=1.10)
            for index, shard in enumerate(shards):
                endpoints = [c.get("projection_wal_bytes") or 0 for campaign in shard["campaigns"] for c in campaign["cycles"]]
                peak = observation.get("peak_bytes", {}).get(f"shard-{index}", 0)
                check(f"shard_{index}_wal_budget", peak>0 and max([peak,*endpoints]) <= 512*1024*1024)
                sizes = [max(campaign["cycles"][i].get("projection_bytes") or 0 for campaign in shard["campaigns"])
                         for i in range(cycles-3,cycles)]
                # The campaign creates fresh 4 KiB-page databases. A constant
                # header-only file is not evidence of steady checkpoint cost.
                check(f"shard_{index}_projection_checkpoint_materialized", min(sizes)>4096,
                      sizes, "all three final main-file snapshots >4096 bytes")
                check(f"shard_{index}_projection_stable", min(sizes)>0 and max(sizes)/min(sizes)<=1.05)
    else:
        check("supported_qualification_profile", False, result.get("schema"))
    return {"passed": all(c["passed"] for c in checks), "checks": checks}
