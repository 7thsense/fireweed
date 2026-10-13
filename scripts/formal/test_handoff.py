"""Independent handoff execution and evidence failure controls."""
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch
import check_handoff as bootstrap
gate = bootstrap.load_runtime()

class HandoffGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.report = gate.run()

    def test_complete_graph_and_replayable_mutants(self):
        self.assertEqual(self.report['schema'], bootstrap.SCHEMA)
        baseline = self.report['baseline'][0]
        self.assertTrue(baseline['complete'])
        self.assertGreater(baseline['states'], 1)
        self.assertEqual(len(baseline['completed_witnesses']), 5)
        self.assertEqual(len(self.report['negative_controls']), 8)
        for control in self.report['negative_controls']:
            gate.helper.replay(gate.model.Handoff(control['fault']),control['trace'],control['property'])

    def test_three_fresh_groups_need_four_complete_recoveries(self):
        model=gate.model.Handoff(); state=model.initial
        actions=['RecoverFullPrefix','BindDeclaredPosition','PrepareNative','ConditionalPublish','PostRecoverFullPrefix','ResolveOriginal']
        actions+=['BindDeclaredPosition','PrepareNative','ConditionalPublish','PostRecoverFullPrefix','ResolveOriginal']*2
        for wanted in actions:
            state=next(after for action,after in model.transitions(state) if action==wanted)
        self.assertEqual((state.group,state.recoveries,state.acknowledged),(3,4,(0,1,2)))
        self.assertEqual(state.originals,(True,True,True))

    def test_non_newer_absence_never_permits_resubmission(self):
        model=gate.model.Handoff(); state=model.initial
        for wanted in ['RecoverFullPrefix','BindDeclaredPosition','PrepareNative','CompetingFence',
                       'ConditionalReject','PostReadWithoutNewFence']:
            state=next(after for action,after in model.transitions(state) if action==wanted)
        self.assertEqual(state.cut[1],state.submitted_epoch)
        protocol=[action for action,_ in model.transitions(state) if action not in ('ForeignPublication','CompetingFence','RacedOriginal')]
        self.assertEqual(protocol,['ReturnUnknown'])

    def test_incomplete_search_cannot_pass(self):
        with self.assertRaises(gate.helper.SearchLimit): gate.run(1)

    def test_damaged_trace_cannot_replay(self):
        control=self.report['negative_controls'][0]
        trace=json.loads(json.dumps(control['trace']));trace[-1]['state']['head']+=99
        with self.assertRaises(ValueError):
            gate.helper.replay(gate.model.Handoff(control['fault']),trace,control['property'])

    def test_missing_physical_publication_cannot_complete_named_witness(self):
        model=gate.model.Handoff();report,graph=gate.helper.explore(model,100000)
        states,edges,parents=graph
        edges=[[(action,dest) for action,dest in row if action!='ConditionalPublish'] for row in edges]
        original=gate.helper.explore
        with patch.object(gate.helper,'explore',lambda spec,cap:(report,(states,edges,parents)) if spec.fault is None else original(spec,cap)):
            with self.assertRaises(AssertionError): gate.run()

    def test_failed_rerun_replaces_previous_pass(self):
        root = Path(__file__).resolve().parents[2]
        output = root / 'target/api007-formal/test-handoff-rerun.json'
        command = [sys.executable, str(root / 'scripts/formal/check_handoff.py'), '--output', str(output)]
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
            bootstrap.output_path(root, Path('/tmp/handoff-proof.json'))
        with self.assertRaises(ValueError):
            gate.model.Handoff('typo')

    def test_abbreviated_output_option_is_refused(self):
        root = Path(__file__).resolve().parents[2]
        output = root / 'target/api007-formal/test-handoff-abbreviation.json'
        command = [sys.executable, str(root / 'scripts/formal/check_handoff.py'), '--output', str(output)]
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
        output = root / 'target/api007-formal/test-handoff-startup.json'
        read = Path.read_bytes
        for dependency in ('handoff_gate.py', 'check_api007.py', 'handoff_models.py'):
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
