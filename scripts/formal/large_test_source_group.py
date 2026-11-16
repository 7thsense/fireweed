"""Large-scale tier of test_source_group: tests over 10 s, run by the `large` CI job."""
import check_source_group as bootstrap
import json
import unittest
from pathlib import Path
from unittest.mock import patch


class LargeSourceGroupGateTests(unittest.TestCase):
    def test_failed_rerun_replaces_success_and_unavailable_model_fails_closed(self):
        root = Path(__file__).resolve().parents[2]
        output = root/'target/api007-formal/test-source-group-rerun.json'
        read = Path.read_bytes
        try:
            self.assertEqual(bootstrap.main(['--output', str(output)]), 0)
            self.assertEqual(bootstrap.main(['--output', str(output), '--max-states', '1']), 1)
            self.assertEqual(json.loads(output.read_text())['status'], 'FAIL')
            output.write_text(json.dumps({'status':'PASS'}))
            def unavailable(path):
                if path.name == 'source_group_models.py': raise FileNotFoundError('injected missing source-group model')
                return read(path)
            with patch.object(Path, 'read_bytes', unavailable):
                self.assertEqual(bootstrap.main(['--output', str(output)]), 1)
            self.assertEqual(json.loads(output.read_text())['status'], 'FAIL')
            output.write_text(json.dumps({'status':'PASS'}))
            self.assertEqual(bootstrap.main(['--output', str(output), '--max-states', 'bad']), 1)
            self.assertEqual(json.loads(output.read_text())['status'], 'FAIL')
        finally: output.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
