"""Independent pack_preparation execution and evidence failure controls."""
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch
import check_pack_preparation as bootstrap
gate = bootstrap.load_runtime()

class PackPreparationGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.report = gate.run()

    def test_complete_graph_and_replayable_mutants(self):
        self.assertEqual(self.report['schema'], bootstrap.SCHEMA)
        baseline = self.report['baseline'][0]
        self.assertTrue(baseline['complete'])
        self.assertGreater(baseline['states'], 1)
        self.assertEqual(len(baseline['completed_witnesses']), 14)
        self.assertEqual(len(self.report['negative_controls']), 18)
        for control in self.report['negative_controls']:
            gate.helper.replay(gate.model.PackPreparation(control['fault']),control['trace'],control['property'])

    def test_shrinking_final_frame_still_stops_at_first_overflow(self):
        report,graph=gate.helper.explore(gate.model.PackPreparation(),100000)
        states,_,_=graph
        outputs=[state for state in states if state.phase=='done' and state.case=='split-shrinking']
        self.assertEqual(len(outputs),1)
        self.assertEqual(len(outputs[0].receipts),1)
        self.assertEqual(outputs[0].clocks,(0,))
        self.assertEqual(outputs[0].calls,2)

    def test_admission_failure_never_becomes_size_fallback(self):
        model=gate.model.PackPreparation();state=model.initial
        for wanted in ['Choose:admission-failure','RejectAdmission']:
            state=next(after for action,after in model.transitions(state) if action==wanted)
        self.assertEqual(state.terminal,'Unknown')
        self.assertFalse(state.admitted)
        self.assertEqual(list(model.transitions(state)),[])

    def test_incomplete_search_cannot_pass(self):
        with self.assertRaises(gate.helper.SearchLimit): gate.run(1)

    def test_damaged_trace_cannot_replay(self):
        control=self.report['negative_controls'][0]
        trace=json.loads(json.dumps(control['trace']));trace[-1]['state']['version']+=99
        with self.assertRaises(ValueError):
            gate.helper.replay(gate.model.PackPreparation(control['fault']),trace,control['property'])

    def test_missing_physical_publication_cannot_complete_named_witness(self):
        model=gate.model.PackPreparation();report,graph=gate.helper.explore(model,100000)
        states,edges,parents=graph
        edges=[[(action,dest) for action,dest in row if action!='PlanGroup'] for row in edges]
        original=gate.helper.explore
        with patch.object(gate.helper,'explore',lambda spec,cap:(report,(states,edges,parents)) if spec.fault is None else original(spec,cap)):
            with self.assertRaises(AssertionError): gate.run()

    def test_failed_rerun_replaces_previous_pass(self):
        root = Path(__file__).resolve().parents[2]
        output = root / 'target/api007-formal/test-pack_preparation-rerun.json'
        command = [sys.executable, str(root / 'scripts/formal/check_pack_preparation.py'), '--output', str(output)]
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
            bootstrap.output_path(root, Path('/tmp/pack_preparation-proof.json'))
        with self.assertRaises(ValueError):
            gate.model.PackPreparation('typo')

    def test_abbreviated_output_option_is_refused(self):
        root = Path(__file__).resolve().parents[2]
        output = root / 'target/api007-formal/test-pack_preparation-abbreviation.json'
        command = [sys.executable, str(root / 'scripts/formal/check_pack_preparation.py'), '--output', str(output)]
        try:
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 0)
            result = subprocess.run(command + ['--out', str(output)], capture_output=True)
            self.assertEqual(result.returncode, 1)
            self.assertIn(b'unrecognized arguments: --out', result.stderr)
            self.assertEqual(json.loads(output.read_text())['status'], 'FAIL')
        finally:
            output.unlink(missing_ok=True)

    def test_executed_runner_body_is_captured(self):
        path = str(Path(gate.__file__).resolve())
        self.assertIn(path, gate.helper.LOADED_SOURCES)
        with patch.dict(gate.helper.LOADED_SOURCES, {path: '0' * 64}):
            with self.assertRaises(ValueError):
                gate.helper.validate_loaded_sources()

    def test_startup_missing_or_invalid_source_replaces_previous_pass(self):
        root = Path(__file__).resolve().parents[2]
        output = root / 'target/api007-formal/test-pack_preparation-startup.json'
        read = Path.read_bytes
        for dependency in ('pack_preparation_gate.py', 'check_api007.py', 'pack_preparation_models.py'):
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
