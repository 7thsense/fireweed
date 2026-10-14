"""Independent continuation execution and evidence failure controls."""
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch
import check_continuation as bootstrap
gate = bootstrap.load_runtime()

class ContinuationGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.report = gate.run()

    def test_complete_graph_and_replayable_mutants(self):
        self.assertEqual(self.report['schema'], bootstrap.SCHEMA)
        baseline = self.report['baseline'][0]
        self.assertTrue(baseline['complete'])
        self.assertGreater(baseline['states'], 1)
        self.assertEqual(len(baseline['completed_witnesses']), 13)
        self.assertEqual(len(self.report['negative_controls']), 17)
        self.assertEqual(len(self.report['baseline'][1]['completed_witnesses']), 16)
        for control in self.report['negative_controls']:
            gate.helper.replay((gate.model.SingleOriginalBinding(control['fault']) if control.get('model') else gate.model.Continuation(control['fault'])),control['trace'],control['property'])

    def test_forged_covered_output_cannot_hide_behind_false_touch(self):
        from dataclasses import replace
        model = gate.model.SingleOriginalBinding()
        before = replace(model.initial,phase='bind',domain=1,expected_domain=0)
        after = replace(before,phase='done',terminal='Covered',touched_authority=False)
        self.assertEqual(model.violation(before,'ForgedCovered',after),'S-DECLARATION')
        before = replace(model.initial,phase='bind',descriptor_present=True,old_present=True)
        after = replace(before,phase='done',terminal='Covered',touched_authority=False,resumed_present=False)
        self.assertEqual(model.violation(before,'ForgedCovered',after),'S-PRESENCE')

    def test_empty_suffix_keeps_historical_epoch(self):
        model=gate.model.Continuation(); state=model.initial
        for wanted in ['Choose:empty','AcquireFresh','VerifyCheckpoint','ResolveCovered']:
            state=next(after for action,after in model.transitions(state) if action==wanted)
        self.assertEqual((state.offset,state.historical_epoch,state.acquired),(2,2,4))
        self.assertTrue(state.acknowledged)

    def test_rewind_and_disappearance_end_unknown(self):
        for case in ('rewind','vanished'):
            model=gate.model.Continuation(); state=model.initial
            for wanted in ['Choose:'+case,'RejectKnownRegression']:
                state=next(after for action,after in model.transitions(state) if action==wanted)
            self.assertEqual(state.terminal,'Unknown')
            self.assertEqual(list(model.transitions(state)),[])

    def test_incomplete_search_cannot_pass(self):
        with self.assertRaises(gate.helper.SearchLimit): gate.run(1)

    def test_damaged_trace_cannot_replay(self):
        control=self.report['negative_controls'][0]
        trace=json.loads(json.dumps(control['trace']));trace[-1]['state']['head']+=99
        with self.assertRaises(ValueError):
            gate.helper.replay((gate.model.SingleOriginalBinding(control['fault']) if control.get('model') else gate.model.Continuation(control['fault'])),trace,control['property'])

    def test_missing_physical_publication_cannot_complete_named_witness(self):
        model=gate.model.Continuation();report,graph=gate.helper.explore(model,100000)
        states,edges,parents=graph
        edges=[[(action,dest) for action,dest in row if action!='AcquireFresh'] for row in edges]
        original=gate.helper.explore
        with patch.object(gate.helper,'explore',lambda spec,cap:(report,(states,edges,parents)) if spec.fault is None else original(spec,cap)):
            with self.assertRaises(AssertionError): gate.run()

    def test_failed_rerun_replaces_previous_pass(self):
        root = Path(__file__).resolve().parents[2]
        output = root / 'target/api007-formal/test-continuation-rerun.json'
        command = [sys.executable, str(root / 'scripts/formal/check_continuation.py'), '--output', str(output)]
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
            bootstrap.output_path(root, Path('/tmp/continuation-proof.json'))
        with self.assertRaises(ValueError):
            gate.model.Continuation('typo')

    def test_abbreviated_output_option_is_refused(self):
        root = Path(__file__).resolve().parents[2]
        output = root / 'target/api007-formal/test-continuation-abbreviation.json'
        command = [sys.executable, str(root / 'scripts/formal/check_continuation.py'), '--output', str(output)]
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
        output = root / 'target/api007-formal/test-continuation-startup.json'
        read = Path.read_bytes
        for dependency in ('continuation_gate.py', 'check_api007.py', 'continuation_models.py'):
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
