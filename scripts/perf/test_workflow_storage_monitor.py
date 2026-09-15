from pathlib import Path
import tempfile
import unittest
from workflow_storage_monitor import WalMonitor, process_memory_sample


class WalMonitorTests(unittest.TestCase):
    def test_peak_survives_restart_and_file_disappearance(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            shard = root / "shard-0"
            shard.mkdir()
            path = shard / "projection.db-wal"
            monitor = WalMonitor(root)
            path.write_bytes(b"x" * 8192)
            monitor.sample()
            path.write_bytes(b"x" * 32)
            monitor.sample()
            path.unlink()
            monitor.sample()
            self.assertEqual(monitor.peaks, {"shard-0": 8192})
            self.assertEqual(monitor.errors, [])

    def test_memory_counters_handle_process_names_with_parentheses(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            process = root / '123'
            process.mkdir()
            (process / 'status').write_text('VmRSS:\t120 kB\nRssAnon:\t80 kB\nVmSwap:\t12 kB\n')
            # Values equal their documented proc stat field number.
            (process / 'stat').write_text('123 (name with ) parens) S ' +
                                         ' '.join(str(i) for i in range(4, 53)))
            (root / 'meminfo').write_text('MemAvailable: 1000 kB\nSwapFree: 500 kB\n')
            sample = process_memory_sample(123, root)
            self.assertEqual(sample['VmRSS_kib'], 120)
            self.assertEqual(sample['RssAnon_kib'], 80)
            self.assertEqual(sample['VmSwap_kib'], 12)
            self.assertEqual(sample['minor_faults'], 10)
            self.assertEqual(sample['major_faults'], 12)
            self.assertEqual(sample['start_ticks'], 22)
            self.assertEqual(sample['host']['MemAvailable_kib'], 1000)

    def test_exited_process_does_not_break_wal_sampling(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as root:
            monitor = WalMonitor(Path(root), pid=123)
            with patch('workflow_storage_monitor.process_memory_sample', side_effect=FileNotFoundError):
                monitor.sample()
            self.assertEqual(monitor.samples, 1)
            self.assertEqual(monitor.memory_samples, [])
            self.assertEqual(monitor.memory_errors, [])
