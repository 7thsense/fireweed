import copy
import unittest
from workflow_capacity_gate import qualify


class QualificationTests(unittest.TestCase):
    def baseline(self):
        return {"exit_code": 0, "filesystem": {"filesystems": [{"fstype": "btrfs"}]},
                "result": {"schema": "primitive-capacity/v1", "cell": "filesystem--turso",
                           "items": 1_000_000, "physical_shards": 8, "aggregate_phases": [
                               {"phase": phase, "records_per_s": 12_000} for phase in
                               ("insert", "enrich_by_key", "schedule_by_id")]}}

    def test_successful_component_gate_and_missing_phase(self):
        report = self.baseline()
        self.assertTrue(qualify(report)["passed"])
        report["result"]["aggregate_phases"].pop()
        self.assertFalse(qualify(report)["passed"])

    def test_ram_and_io_override_cannot_qualify(self):
        for change in ({"projection_filesystem": {"filesystems": [{"fstype": "tmpfs"}]}},
                       {"diagnostic_provenance": {"dependency": "experimental engine"}},
                       {"diagnostics": {"LD_PRELOAD": "/tmp/override.so"}}, {"exit_code": 1}):
            report = self.baseline()
            report.update(change)
            self.assertFalse(qualify(report)["passed"])

    def test_workflow_average_cannot_hide_slow_cycle_or_memory_growth(self):
        report = self.baseline()
        report["result"] = {"schema": "workflow-capacity/v4", "cell": "filesystem--turso",
                            "profile": "Mutable", "atomic_original_row_mutation": True,
                            "dispatch": "shared-normal-claim", "faults": True, "includes_purge": True,
                            "cycles": 3, "items": 400_000, "physical_shards": 2, "completed_lifecycles_per_s": 6000,
                            "shards": [{"cycles": [{"items": 200_000, "wall_s": 30,
                                                      "process_rss_kib": 1000, "projection_bytes": 1000}
                                                     for _ in range(3)]} for _ in range(2)]}
        self.assertTrue(qualify(report)["passed"])
        with_wal = copy.deepcopy(report)
        with_wal["result"]["schema"] = "workflow-capacity/v5"
        self.assertFalse(qualify(with_wal)["passed"], "v5 requires WAL evidence")
        for shard in with_wal["result"]["shards"]:
            for cycle in shard["cycles"]:
                cycle["projection_wal_bytes"] = 1000
        self.assertTrue(qualify(with_wal)["passed"])
        with_wal["result"]["shards"][0]["cycles"][2]["projection_wal_bytes"] = 2000
        self.assertFalse(qualify(with_wal)["passed"], "stable DB size must not hide growing WAL")
        slow = copy.deepcopy(report)
        slow["result"]["shards"][0]["cycles"][2]["wall_s"] = 100
        self.assertFalse(qualify(slow)["passed"])
        report["result"]["shards"][0]["cycles"][2]["process_rss_kib"] = 1200
        self.assertFalse(qualify(report)["passed"])


if __name__ == "__main__":
    unittest.main()
