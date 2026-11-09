"""Large-scale tier of test_owner: tests over 10 s, run by the `large` CI job."""
import check_owner as bootstrap
import json
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from test_owner import gate, model


class LargeOwnerGateTests(unittest.TestCase):
    def test_startup_missing_or_invalid_source_replaces_previous_pass(self):
        root = Path(__file__).resolve().parents[2]
        output = root / 'target/api007-formal/test-owner-startup.json'
        read = Path.read_bytes
        for dependency in ('owner_gate.py', 'check_api007.py', 'owner_models.py'):
            for invalid in (False, True):
                try:
                    self.assertEqual(bootstrap.main(['--output', str(output)]), 0)
                    def damaged(path):
                        if path.name == dependency:
                            if invalid:
                                return b'this is not valid Python!'
                            raise FileNotFoundError('injected unavailable dependency')
                        return read(path)
                    with patch.object(Path, 'read_bytes', damaged):
                        self.assertEqual(bootstrap.main(['--output', str(output)]), 1)
                    self.assertEqual(json.loads(output.read_text())['status'], 'FAIL')
                finally:
                    output.unlink(missing_ok=True)
    def test_failed_rerun_replaces_previous_pass(self):
        root = Path(__file__).resolve().parents[2]
        output = root / 'target/api007-formal/test-owner-rerun.json'
        command = [sys.executable, str(root / 'scripts/formal/check_owner.py'), '--output', str(output)]
        try:
            for suffix, status in [([], 0), (['--max-states', '1'], 1), ([], 0),
                                   (['--max-states', 'bad'], 1), ([], 0), (['--out', str(output)], 1)]:
                self.assertEqual(subprocess.run(command + suffix, capture_output=True).returncode, status)
                self.assertEqual(json.loads(output.read_text())['status'], 'PASS' if status == 0 else 'FAIL')
        finally:
            output.unlink(missing_ok=True)
    def test_insensitive_control_fails_the_gate(self):
        original = gate.helper.explore
        def blind(spec, cap):
            return original(model.Owner() if spec.fault == 'ack_without_recheck' else spec, cap)
        with patch.object(gate.helper, 'explore', blind):
            with self.assertRaises(AssertionError):
                gate.run()
    def test_every_reachable_state_can_settle(self):
        for scenario, limits in model.SCENARIOS.items():
            specification = model.Owner(scenario=scenario)
            _, (states, edges, _) = gate.helper.explore(specification, 100000)
            self.assertEqual(gate.stuck_states(states, edges, limits['horizon']), 0)
        edges = [[(a, d) for a, d in row if not a.startswith('Process')] for row in edges]
        self.assertGreater(gate.stuck_states(states, edges, limits['horizon']), 0)


if __name__ == "__main__":
    unittest.main()
