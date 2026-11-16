"""Bounded Check gate controls, independent of existing publication tests."""
from dataclasses import replace
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

import check_conditions as bootstrap
gate = bootstrap.load_runtime()


class ConditionGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.report = gate.run()

    def test_full_graphs_witnesses_and_replayable_negative_controls(self):
        self.assertEqual(len(self.report['baseline']), 6)
        self.assertEqual(len(self.report['negative_controls']), 8)
        for baseline in self.report['baseline']:
            self.assertTrue(baseline['complete'])
            self.assertGreater(baseline['states'], 1)
            self.assertGreater(baseline['edges'], baseline['states'])
            self.assertGreater(baseline['frontiers'], 0)
            self.assertGreaterEqual(len(baseline['completed_witnesses']), 12)
        for control in self.report['negative_controls']:
            model = gate.model.Conditions(tuple(control['configuration']), control['fault'])
            gate.helper.replay(model, control['trace'], control['property'])

    def test_incomplete_search_cannot_pass(self):
        with self.assertRaises(gate.helper.SearchLimit):
            gate.run(1)

    def test_loaded_model_bytes_are_validated(self):
        path = str(gate.model_path)
        with patch.dict(gate.helper.LOADED_SOURCES, {path: '0' * 64}):
            with self.assertRaises(ValueError):
                gate.helper.validate_loaded_sources()

    def test_state_delete_recreate_never_matches_old_version(self):
        model = gate.model.Conditions(('mixed', 'present'))
        state = model.initial
        for wanted in ['DeleteState', 'RecreateState']:
            state = next(after for action, after in model.transitions(state) if action == wanted)
        self.assertEqual(state.state_hwm, 3)
        self.assertEqual(model.decision(model.metadata(state), 0), 'version-conflict')

    def test_observation_oracle_rejects_recomputed_original(self):
        model = gate.model.Conditions(('checks-only', 'present'))
        state = model.initial
        for wanted in ['InvokeEqual', 'ReadCut', 'SampleFreshTlease', 'PublishReceipt',
                       'ResolveOriginal', 'ExactExpiry', 'InvokeEqual']:
            state = next(after for action, after in model.transitions(state) if action == wanted)
        _, after = next((action, after) for action, after in model.transitions(state)
                        if action == 'ResolveOriginal')
        wrong = replace(after, observations=state.observations + ((state.body, 'stale-lease'),))
        self.assertEqual(model.violation(state, 'ResolveOriginal', wrong), 'C-REPLAY')

    def test_ordered_outcome_truth_table_and_independent_oracle(self):
        model = gate.model.Conditions(('mixed', 'present'))
        for version, token, time, present, hwm, expected in [
            (1, 0, 0, True, 1, 'committed'),
            (2, 0, 0, True, 1, 'version-conflict'),
            (2, 1, 0, True, 1, 'stale-lease'),
            (2, 0, 1, True, 1, 'stale-lease'),
            (1, 1, 0, False, 2, 'stale-lease'),
            (1, 0, 1, False, 2, 'stale-lease'),
            (1, 0, 0, False, 2, 'not-found'),
            (1, 0, 0, True, 3, 'version-conflict'),
        ]:
            cut = (1, 0, version, token, present, hwm)
            self.assertEqual(model.decision(cut, time), expected)
            self.assertEqual(model.expected_outcome(cut, time), expected)
        for kind in ('absent', 'tombstoned'):
            absent = gate.model.Conditions(('mixed', kind))
            self.assertEqual(absent.expected_outcome(absent.metadata(absent.initial), 0), 'committed')
            present_cut = (1, 0, 1, 0, True, 2)
            self.assertEqual(absent.decision(present_cut, 0), 'already-exists')
            self.assertEqual(absent.expected_outcome(present_cut, 0), 'already-exists')
        state = replace(model.initial, phase='publish', cut=model.metadata(model.initial),
                        lease_time=1, planned='committed')
        with patch.object(model, 'decision', return_value='committed'):
            after = next(after for action, after in model.transitions(state) if action == 'PublishReceipt')
            self.assertEqual(model.violation(state, 'PublishReceipt', after), 'C-PREDICATE')

    def test_environment_frontier_does_not_hide_started_protocol(self):
        model = gate.model.Conditions(('mixed', 'present'))
        state = model.initial
        for wanted in ['ExactExpiry', 'Fence', 'InvokeEqual', 'ReadCut',
                       'SampleFreshTlease', 'PublishReceipt', 'ResolveOriginal']:
            if state.environmental_steps == 2:
                self.assertTrue(model.frontier(state))
            state = next(after for action, after in model.transitions(state) if action == wanted)
        self.assertEqual(state.invocation, 1)
        self.assertEqual(state.observations[-1][1], 'stale-lease')

    def test_damaged_trace_cannot_replay(self):
        control = self.report['negative_controls'][0]
        trace = json.loads(json.dumps(control['trace']))
        trace[-1]['state']['head'] += 99
        with self.assertRaises(ValueError):
            gate.helper.replay(gate.model.Conditions(tuple(control['configuration']), control['fault']),
                               trace, control['property'])

    def test_failed_rerun_replaces_previous_pass(self):
        root = Path(__file__).resolve().parents[2]
        output = root / 'target/api007-formal/test-condition-rerun.json'
        command = [sys.executable, str(root / 'scripts/formal/check_conditions.py'), '--output', str(output)]
        try:
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 0)
            self.assertEqual(json.loads(output.read_text())['status'], 'PASS')
            self.assertEqual(subprocess.run(command + ['--max-states', '1'], capture_output=True).returncode, 1)
            self.assertEqual(json.loads(output.read_text())['status'], 'FAIL')
        finally:
            output.unlink(missing_ok=True)

    def test_output_escape_and_unknown_configuration_are_refused(self):
        root = Path(__file__).resolve().parents[2]
        with self.assertRaises(ValueError):
            gate.helper.output_path(root, Path('/tmp/condition-proof.json'))
        with self.assertRaises(ValueError):
            gate.model.Conditions(('typo', 'present'))
        with self.assertRaises(ValueError):
            gate.model.Conditions(('checks-only', 'present'), 'typo')

    def test_executed_runner_body_is_captured(self):
        path = str(Path(gate.__file__).resolve())
        self.assertIn(path, gate.helper.LOADED_SOURCES)
        with patch.dict(gate.helper.LOADED_SOURCES, {path: '0' * 64}):
            with self.assertRaises(ValueError):
                gate.helper.validate_loaded_sources()

    def test_startup_missing_or_invalid_source_replaces_previous_pass(self):
        root = Path(__file__).resolve().parents[2]
        output = root / 'target/api007-formal/test-condition-startup.json'
        read = Path.read_bytes
        for dependency in ('condition_gate.py', 'check_api007.py', 'condition_models.py'):
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


if __name__ == '__main__':
    unittest.main()
