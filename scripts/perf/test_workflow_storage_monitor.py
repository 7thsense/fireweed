from pathlib import Path
import tempfile
import unittest
from workflow_storage_monitor import WalMonitor


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
