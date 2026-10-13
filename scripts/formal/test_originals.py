"""Independent cache-authority truth table and fail-closed evidence controls."""
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch
import check_originals as bootstrap
gate = bootstrap.load_runtime()

class OriginalGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.report = gate.run()

    def test_complete_graph_and_replayable_mutants(self):
        report = self.report
        self.assertEqual(len(report['baseline']), 1)
        baseline = report['baseline'][0]
        self.assertTrue(baseline['complete'])
        self.assertGreater(baseline['states'], 1)
        self.assertGreater(baseline['frontiers'], 0)
        self.assertEqual(len(baseline['completed_witnesses']), 11)
        self.assertEqual(len(report['negative_controls']), 8)
        for control in report['negative_controls']:
            gate.helper.replay(gate.model.Originals(control['fault']), control['trace'], control['property'])

    def test_independent_ordered_truth_table(self):
        model = gate.model.Originals()
        for case, expected in [
            ((True,True,0,0,0,0,0,0,0,True), 'replay'),
            ((False,False,1,1,1,1,2,2,1,True), 'fresh'),
            ((False,True,0,0,0,0,0,0,0,True), 'unknown'),
            ((True,False,0,0,0,0,0,0,0,True), 'unknown'),
            ((True,True,0,1,0,1,0,0,0,True), 'unknown'),
            ((True,True,0,0,0,1,2,2,1,True), 'conflict'),
            ((True,True,0,0,0,0,1,0,0,True), 'unknown'),
            ((True,True,0,0,0,0,0,1,0,True), 'unknown'),
            ((True,True,0,0,0,0,0,0,1,True), 'unknown'),
            ((True,True,0,0,0,0,0,0,0,False), 'unknown'),
        ]:
            self.assertEqual(model.expected(case), expected)

    def test_missing_exact_case_selector_cannot_witness(self):
        model = gate.model.Originals()
        report, graph = gate.helper.explore(model, 100000)
        case = gate.model.CASES[1]
        states, edges, predecessors = graph
        edges = [[(action, dest) for action, dest in row if action != 'SelectCase-1']
                 for row in edges]
        graph = (states, edges, predecessors)
        sequence = ['SelectCase-1', 'CompleteClassifications', 'ReturnUnknown']
        # The old case-blind predicate incorrectly substitutes SelectCase-10.
        wrong = gate.helper.witness(model, graph, sequence,
            lambda state: state.phase == 'done' and state.result == 'unknown')
        self.assertNotEqual(wrong['trace'][-1]['state']['case'], list(case))
        original_explore = gate.helper.explore
        def missing_case(specification, max_states):
            return (report, graph) if specification.fault is None else original_explore(specification, max_states)
        with patch.object(gate.helper, 'explore', missing_case):
            with self.assertRaises(AssertionError):
                gate.run()

    def test_incomplete_search_cannot_pass(self):
        with self.assertRaises(gate.helper.SearchLimit):
            gate.run(1)

    def test_damaged_trace_cannot_replay(self):
        control = self.report['negative_controls'][0]
        trace = json.loads(json.dumps(control['trace']))
        trace[-1]['state']['publications'] += 99
        with self.assertRaises(ValueError):
            gate.helper.replay(gate.model.Originals(control['fault']), trace, control['property'])

    def test_failed_rerun_replaces_previous_pass(self):
        root = Path(__file__).resolve().parents[2]
        output = root / 'target/api007-formal/test-original-rerun.json'
        command = [sys.executable, str(root / 'scripts/formal/check_originals.py'), '--output', str(output)]
        try:
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 0)
            self.assertEqual(json.loads(output.read_text())['status'], 'PASS')
            self.assertEqual(subprocess.run(command + ['--max-states', '1'], capture_output=True).returncode, 1)
            self.assertEqual(json.loads(output.read_text())['status'], 'FAIL')
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 0)
            self.assertEqual(json.loads(output.read_text())['status'], 'PASS')
            self.assertEqual(subprocess.run(command + ['--max-states', 'bad'], capture_output=True).returncode, 1)
            self.assertEqual(json.loads(output.read_text())['status'], 'FAIL')
        finally:
            output.unlink(missing_ok=True)

    def test_output_escape_and_unknown_fault_are_refused(self):
        root = Path(__file__).resolve().parents[2]
        with self.assertRaises(ValueError):
            bootstrap.output_path(root, Path('/tmp/original-proof.json'))
        with self.assertRaises(ValueError):
            gate.model.Originals('typo')

    def test_executed_runner_body_is_captured(self):
        path = str(Path(gate.__file__).resolve())
        self.assertIn(path, gate.helper.LOADED_SOURCES)
        with patch.dict(gate.helper.LOADED_SOURCES, {path: '0' * 64}):
            with self.assertRaises(ValueError):
                gate.helper.validate_loaded_sources()

    def test_startup_missing_or_invalid_source_replaces_previous_pass(self):
        root = Path(__file__).resolve().parents[2]
        output = root / 'target/api007-formal/test-original-startup.json'
        read = Path.read_bytes
        for dependency in ('original_gate.py', 'check_api007.py', 'original_models.py'):
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
