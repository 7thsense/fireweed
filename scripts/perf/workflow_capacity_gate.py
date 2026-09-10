"""Performance acceptance for the original-row workload and component ladder."""

def qualify(report):
    result = report.get("result", {})
    checks = []

    def check(name, passed, actual=None, required=None):
        checks.append({"name": name, "passed": bool(passed), "actual": actual, "required": required})

    check("successful_correctness_run", report.get("exit_code") == 0)
    check("filesystem_log_and_turso", result.get("cell") == "filesystem--turso")
    check("storage_evidence_present", bool(report.get("filesystem")))
    check("physical_sharding", result.get("physical_shards", 0) >= 2)
    for name in ("filesystem", "projection_filesystem"):
        if name in report:
            mounts = report[name].get("filesystems", [])
            check(name + "_on_disk", bool(mounts) and all(m.get("fstype") not in ("tmpfs", "ramfs", None) for m in mounts))
    check("no_external_io_override", not report.get("diagnostics", {}).get("LD_PRELOAD"))
    if result.get("schema") == "primitive-capacity/v1":
        check("million_resident_rows", result.get("items", 0) >= 1_000_000, result.get("items"), 1_000_000)
        phases = {p["phase"]: p["records_per_s"] for p in result.get("aggregate_phases", [])}
        for name in ("insert", "enrich_by_key", "schedule_by_id"):
            rate = phases.get(name, 0)
            check(name, rate >= 10_000, rate, 10_000)
    elif result.get("schema") == "workflow-capacity/v4":
        check("original_row_workflow", result.get("profile") == "Mutable" and result.get("atomic_original_row_mutation") is True and result.get("dispatch") == "shared-normal-claim")
        check("faults_and_retention", result.get("faults") is True and result.get("includes_purge") is True)
        cycles = result.get("cycles", 0)
        check("at_least_three_cycles", cycles >= 3, cycles, 3)
        count = result.get("items", 0) * cycles
        check("million_complete_workflows", count >= 1_000_000, count, 1_000_000)
        rate = result.get("completed_lifecycles_per_s", 0)
        check("overall_workflows_per_s", rate >= 5_000, rate, 5_000)
        shards = result.get("shards", [])
        check("complete_cycle_reports", bool(shards) and len(shards) == result.get("physical_shards") and all(len(s.get("cycles", [])) == cycles for s in shards))
        if shards and cycles >= 3 and all(len(s.get("cycles", [])) == cycles for s in shards):
            # Each shard must sustain its fair share in every cycle, preventing
            # a fast shard or initial burst from hiding starvation or a slowdown.
            for index in range(cycles):
                rows = [s["cycles"][index] for s in shards]
                equivalent_rate = min(c["items"] / c["wall_s"] for c in rows) * len(shards)
                check(f"cycle_{index}_slowest_shard_equivalent_rate", equivalent_rate >= 5_000, equivalent_rate, 5_000)
            rss = [max(s["cycles"][i].get("process_rss_kib") or 0 for s in shards) for i in range(cycles - 3, cycles)]
            rss_growth = (max(rss) / min(rss) - 1) if min(rss) > 0 else None
            check("last_three_cycles_rss_stable", rss_growth is not None and rss_growth <= .10, rss_growth, "<=10% range")
            for index, shard in enumerate(shards):
                sizes = [c.get("projection_bytes") or 0 for c in shard["cycles"][-3:]]
                growth = (max(sizes) / min(sizes) - 1) if min(sizes) > 0 else None
                check(f"shard_{index}_projection_stable", growth is not None and growth <= .05, growth, "<=5% range")
    else:
        check("supported_qualification_profile", False, result.get("schema"))
    return {"passed": all(c["passed"] for c in checks), "checks": checks}
