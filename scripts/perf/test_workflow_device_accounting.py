import unittest

from workflow_device_accounting import summarize


class DeviceAccountingTests(unittest.TestCase):
    def test_counter_units_and_instantaneous_depth(self):
        before = [100] * 17
        after = [100] * 17
        # Two seconds: 20 writes, 80 KiB, 100 ms/write, depth integral 4 s.
        for index, increment in {4: 20, 6: 160, 7: 2000, 9: 1000,
                                 10: 4000, 15: 4, 16: 20}.items():
            after[index] += increment
        before[8], after[8] = 3, 0  # Gauge decreases are valid.
        rows = [{"monotonic_s": t, "process_cpu_s": 1, "nvme0n1": counters}
                for t, counters in [(1, before), (3, after)]]
        report = summarize(rows)
        self.assertEqual(report["write_iops"], 10)
        self.assertEqual(report["write_kib_per_request"], 4)
        self.assertEqual(report["mean_write_request_ms"], 100)
        self.assertEqual(report["weighted_mean_inflight"], 2)
        self.assertEqual(report["busy_percent_not_capacity_utilization"], 50)
        self.assertEqual(report["mean_flush_request_ms"], 5)
        self.assertIsNone(report["mean_read_request_ms"])
        after[4] = 99
        with self.assertRaisesRegex(ValueError, "reset/wrap"):
            summarize(rows)

    def test_missing_or_unordered_samples_fail(self):
        with self.assertRaises(ValueError):
            summarize([])
        row = {"monotonic_s": 1, "process_cpu_s": 1, "nvme0n1": [0] * 17}
        with self.assertRaisesRegex(ValueError, "non-increasing"):
            summarize([row, row])


if __name__ == "__main__":
    unittest.main()
