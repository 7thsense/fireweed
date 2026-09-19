import copy
import unittest
from workflow_capacity_gate import qualify


class QualificationTests(unittest.TestCase):
    def baseline(self):
        return {"dirty": False, "head": "a" * 40, "binary_sha256": "b" * 64, "exit_code": 0, "filesystem": {"filesystems": [{"fstype": "btrfs"}]},
                "result": {"schema": "primitive-capacity/v1", "cell": "s3--turso",
                           "items": 1_000_000, "physical_shards": 8, "aggregate_phases": [
                               {"phase": phase, "records": 1_000_000, "wall_window_s": 1_000_000 / 12_000, "records_per_s": 12_000} for phase in
                               ("insert", "enrich_by_key", "schedule_by_id", "claim_and_complete", "purge")]}}

    def test_successful_component_gate_and_missing_phase(self):
        report = self.baseline()
        self.assertTrue(qualify(report)["passed"])
        report["result"]["aggregate_phases"].pop()
        self.assertFalse(qualify(report)["passed"])

    def test_every_component_requires_complete_work_and_consistent_rate(self):
        for index in range(5):
            missing = self.baseline()
            missing["result"]["aggregate_phases"].pop(index)
            self.assertFalse(qualify(missing)["passed"], (index, "missing"))
            for field, value in [("records", 999_999), ("records", 0),
                                 ("wall_window_s", 0), ("wall_window_s", float("nan")),
                                 ("wall_window_s", float("inf")), ("wall_window_s", 200),
                                 ("records_per_s", 9999), ("records_per_s", 24_000),
                                 ("records_per_s", float("inf")), ("records_per_s", float("nan"))]:
                report = self.baseline()
                report["result"]["aggregate_phases"][index][field] = value
                self.assertFalse(qualify(report)["passed"], (index, field, value))
            at_floor = self.baseline()
            at_floor["result"]["aggregate_phases"][index].update(
                wall_window_s=100, records_per_s=10_000)
            self.assertTrue(qualify(at_floor)["passed"], (index, "exact floor"))

    def test_missing_or_malformed_identity_fails_without_crashing(self):
        for field, check in (("head", "source_identity"), ("binary_sha256", "binary_identity")):
            for value in (None, 123, [], {}, "", "not-a-hash"):
                with self.subTest(field=field, value=value):
                    report = self.baseline()
                    report[field] = value
                    result = qualify(report)
                    self.assertFalse(result["passed"])
                    self.assertFalse(next(c["passed"] for c in result["checks"] if c["name"] == check))

    def test_primitive_payload_identity_is_explicit(self):
        for label in ("repeated_padding", "campaign_varied"):
            report = self.baseline()
            report["result"].update(payload_workload=label, payload_bytes=1024,
                                    initial_payload_bytes=935_000_000,
                                    payload_replacement_bytes=960_000_000)
            self.assertTrue(qualify(report)["passed"])
        for field, value in [("payload_bytes", 1023), ("initial_payload_bytes", 100),
                             ("payload_replacement_bytes", 935_000_000)]:
            broken = copy.deepcopy(report)
            broken["result"][field] = value
            self.assertFalse(qualify(broken)["passed"], field)
        report["result"]["payload_workload"] = "unidentified"
        self.assertFalse(qualify(report)["passed"])

    def test_ram_and_io_override_cannot_qualify(self):
        for change in ({"projection_filesystem": {"filesystems": [{"fstype": "tmpfs"}]}},
                       {"diagnostic_provenance": {"dependency": "experimental engine"}},
                       {"diagnostics": {"LD_PRELOAD": "/tmp/override.so"}}, {"exit_code": 1}):
            report = self.baseline()
            report.update(change)
            self.assertFalse(qualify(report)["passed"])

    def workflow(self):
        report = self.baseline()
        report["process_wall_s"] = 100
        report["projection_wal_observation"] = {"interval_ms": 100, "samples": 1000,
            "errors": [], "peak_bytes": {"shard-0": 3000, "shard-1": 3000}}
        report["result"] = {"schema": "workflow-capacity/v6", "cell": "s3--turso",
            "profile": "Mutable", "atomic_original_row_mutation": True,
            "dispatch": "shared-normal-claim", "faults": True, "includes_purge": True,
            "cycles": 3, "items": 400_000, "physical_shards": 2, "completed_lifecycles_per_s": 9500,
            "shards": [{"cycles": [{"items": 200_000, "wall_s": 30, "delivered": 190_000,
                "failed": 10_000, "complete": 190_000, "pending": 0, "leased": 0,
                "process_rss_kib": 1000, "projection_bytes": 1000, "projection_wal_bytes": 1000}
                for _ in range(3)]} for _ in range(2)]}
        return report

    def test_workflow_rejects_contradictions_and_stale_provenance(self):
        report = self.workflow()
        self.assertTrue(qualify(report)["passed"])
        for key, value in [("dirty", True), ("dirty", None), ("head", ""), ("binary_sha256", "")]:
            broken = copy.deepcopy(report); broken[key] = value
            self.assertFalse(qualify(broken)["passed"], key)
        for key, value in [("pending", 1), ("leased", 1), ("delivered", 0), ("failed", 0),
                           ("complete", 0), ("items", 1), ("wall_s", 0), ("wall_s", float("nan")),
                           ("wall_s", 100), ("process_rss_kib", 2000), ("projection_bytes", 2000)]:
            broken = copy.deepcopy(report); broken["result"]["shards"][0]["cycles"][2][key] = value
            self.assertFalse(qualify(broken)["passed"], (key, value))
        for rate in [9499.99, float("inf"), float("nan")]:
            broken = copy.deepcopy(report); broken["result"]["completed_lifecycles_per_s"] = rate
            self.assertFalse(qualify(broken)["passed"])
        for schema in ["workflow-capacity/v4", "workflow-capacity/v5"]:
            broken = copy.deepcopy(report); broken["result"]["schema"] = schema
            self.assertFalse(qualify(broken)["passed"], schema)

    def test_workflow_requires_sampled_wal_budget(self):
        report = self.workflow()
        for samples in [0, 2]:
            broken = copy.deepcopy(report); broken["projection_wal_observation"]["samples"] = samples
            self.assertFalse(qualify(broken)["passed"])
        for peaks in [{"shard-0": 3000}, {"shard-0": 513 * 1024 * 1024, "shard-1": 3000}]:
            broken = copy.deepcopy(report); broken["projection_wal_observation"]["peak_bytes"] = peaks
            self.assertFalse(qualify(broken)["passed"])


    def test_campaign_requires_independent_outcomes_residency_and_stretch_rate(self):
        report = self.baseline()
        report["process_wall_s"] = 210
        report["projection_wal_observation"] = {"interval_ms":100,"samples":2100,"errors":[],
            "peak_bytes":{"shard-0":3000,"shard-1":3000}}
        shards=[]
        for shard in range(2):
            campaigns=[]
            for campaign in range(2):
                ids=range(shard+campaign*2,1_000_000,4)
                n=len(ids); retries=sum(i%19==0 for i in ids); failed=sum(i%31==0 for i in ids)
                row={"items":n,"verified":n,"purged":n,"failed":failed,"delivered":n-failed,
                    "pending":0,"leased":0,"retries":retries,"claims":3*n+retries,
                    "purge_batches":(n+7999)//8000,"max_purge_batch":min(n,8000),
                    "initial_payload_bytes":n*1024,"payload_replacements":2*n,"payload_replacement_bytes":2*n*1100,
                    "claim_batches":(3*n+retries+999)//1000,"mutation_batches":(3*n+retries+999)//1000,"max_claim_batch":1000,
                    "handler_rows":[n,n,n+retries],"max_handler_batch":[500,200,500],
                    "due_to_claim_max_us":1_000_000,"wall_s":70,"progress_reads":70,"progress_p95_s":0.1,
                    "process_rss_kib":1000,"projection_bytes":65536,"projection_wal_bytes":2000}
                campaigns.append({"campaign":campaign,"cycles":[copy.deepcopy(row) for _ in range(3)]})
            shards.append({"shard":shard,"campaigns":campaigns})
        report["result"]={"schema":"campaign-capacity/v3","enrichment_storage":"payload","payload_bytes":1024,"batch":1000,"purge_batch":8000,"lease_ms":3600000,"request_id_retention_ms":3600000,"cycle_clock_step_s":7200,"cell":"s3--turso","physical_shards":2,
            "campaigns":2,"cycles":3,"items":1_000_000,"resident_backlog":1_000_000,
            "scheduled_windows":4,"stage_limits":[500,200,500],"faults":True,"includes_purge":True,
            "progress_interval_ms":1000,"completed_lifecycles_per_s":11000,"shards":shards}
        self.assertTrue(qualify(report)["passed"])
        self.assertFalse(qualify(report,12500)["passed"])
        report["result"]["completed_lifecycles_per_s"]=13000
        self.assertTrue(qualify(report,12500)["passed"])
        for key,value in [("verified",0),("purged",0),("retries",0),("claims",0),("failed",0),
                          ("progress_reads",0),("progress_p95_s",2.0),("max_handler_batch",[500,500,500]),
                          ("due_to_claim_max_us",61_000_000),("payload_replacements",0),
                          ("payload_replacement_bytes",0),("initial_payload_bytes",0),("mutation_batches",0),
                          ("purge_batches",0),("max_purge_batch",9000)]:
            broken=copy.deepcopy(report);broken["result"]["shards"][0]["campaigns"][0]["cycles"][0][key]=value
            self.assertFalse(qualify(broken)["passed"],key)
        # A constant header-only file has not exercised checkpoint backfill,
        # even when throughput, WAL budget, and size-range checks all pass.
        for header_bytes in [0, 4096]:
            unmaterialized = copy.deepcopy(report)
            for shard in unmaterialized["result"]["shards"]:
                for campaign in shard["campaigns"]:
                    for row in campaign["cycles"]:
                        row["projection_bytes"] = header_bytes
            self.assertFalse(qualify(unmaterialized, 12500)["passed"], header_bytes)
        metadata=copy.deepcopy(report);metadata["result"]["enrichment_storage"]="row_metadata"
        for shard in metadata["result"]["shards"]:
            for campaign in shard["campaigns"]:
                for row in campaign["cycles"]:
                    row["payload_replacements"]=0;row["payload_replacement_bytes"]=0
        self.assertTrue(qualify(metadata,12500)["passed"])
        for key,value in [("priority_workload","unknown"),("enrichment_storage","unknown"),("payload_bytes",128),("schema","campaign-capacity/v1")]:
            broken=copy.deepcopy(metadata);broken["result"][key]=value
            self.assertFalse(qualify(broken)["passed"],key)
        broken=copy.deepcopy(report);broken["result"]["resident_backlog"]=500_000
        self.assertFalse(qualify(broken)["passed"])


if __name__ == "__main__":
    unittest.main()
