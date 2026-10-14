"""Finite state-preparation graph controls and captured fail-closed evidence runner."""
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch
import check_state_preparation as bootstrap
gate = bootstrap.load_runtime()

class StatePreparationGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.report = gate.run()
    def test_complete_graph_and_replayable_mutants(self):
        report = self.report['baseline'][0]
        self.assertTrue(report['complete'])
        self.assertGreater(report['states'],1)
        self.assertEqual(len(report['completed_witnesses']),10)
        self.assertEqual(len(self.report['negative_controls']),10)
        for control in self.report['negative_controls']:
            gate.helper.replay(gate.model.StatePreparation(control['fault']),control['trace'],control['property'])
    def test_work_knowledge_and_replayable_admission_controls(self):
        report = self.report['baseline'][1]
        self.assertTrue(report['complete'])
        self.assertGreater(report['states'],1)
        self.assertEqual(len(report['completed_witnesses']),11)
        self.assertEqual(len(self.report['work_negative_controls']),12)
        for control in self.report['work_negative_controls']:
            gate.helper.replay(gate.model.WorkPreparation(control['fault']),control['trace'],control['property'])
    def test_correct_unknown_cannot_mask_unsafe_work_reads(self):
        model = gate.model.WorkPreparation()
        for case, flag, expected in [
            ((0,0,1,1,0,0,0), 'touched', 'W-SCOPE'),
            ((0,0,1,0,1,0,0), 'fields_loaded', 'W-METADATA'),
        ]:
            for phase in ['fields', 'done']:
                state = gate.model.WorkState(case=case,phase=phase,result='unknown' if phase == 'done' else '',**{flag:True})
                self.assertEqual(model.violation(model.initial,'UnsafeReadThenUnknown',state),expected)
        for fault in ['read_foreign_work_then_unknown','read_bad_metadata_fields_then_unknown']:
            mutant = gate.model.WorkPreparation(fault)
            case = (0,0,1,1,0,0,0) if 'foreign' in fault else (0,0,1,0,1,0,0)
            state = gate.model.WorkState(case=case,phase='scope' if 'foreign' in fault else 'metadata',touched='foreign' not in fault)
            action, bad_read = next(mutant.transitions(state))
            self.assertEqual(mutant.violation(state,action,bad_read),gate.model.WORK_FAULTS[fault])
            action, terminal = next(mutant.transitions(bad_read))
            self.assertEqual(terminal.result,'unknown')
            self.assertEqual(mutant.violation(bad_read,action,terminal),gate.model.WORK_FAULTS[fault])
    def test_work_classification_uses_source_needs_and_not_cache_size(self):
        expected = gate.model.WorkPreparation.expected
        self.assertEqual(expected((0,1,2,1,1,2,2)), 'original')
        self.assertEqual(expected((1,0,0,0,0,2,2)), 'prepared')
        self.assertEqual(expected((1,0,1,0,0,0,2)), 'prepared')
        self.assertEqual(expected((1,0,2,0,0,0,2)), 'unknown')
    def test_original_precedes_every_current_state_touch(self):
        control=next(c for c in self.report['negative_controls'] if c['fault']=='read_before_original')
        terminal=control['trace'][-1]
        self.assertEqual(terminal['action'],'FetchOldFieldsBeforeOriginal')
        self.assertTrue(terminal['state']['current_state_touched'])
        self.assertTrue(terminal['state']['fields_loaded'])
        self.assertGreaterEqual(terminal['state']['case'][2],2)
        self.assertEqual(tuple(terminal['state']['case'][:2]),(0,1))
    def test_independent_scope_and_classification_oracle(self):
        self.assertEqual(gate.model.StatePreparation.expected((0,1,2,1,1,2,2)), 'original')
        self.assertEqual(gate.model.StatePreparation.expected((1,1,2,0,0,0,2)), 'prepared')
        self.assertEqual(gate.model.StatePreparation.expected((1,0,1,0,0,2,2)), 'prepared')
        self.assertEqual(gate.model.StatePreparation.expected((0,0,3,0,0,2,0)), 'unknown')
    def test_incomplete_search_and_damaged_trace_fail(self):
        with self.assertRaises(gate.helper.SearchLimit): gate.run(1)
        control=self.report['negative_controls'][0]
        trace=json.loads(json.dumps(control['trace']));trace[-1]['state']['body_loaded']=not trace[-1]['state']['body_loaded']
        with self.assertRaises(ValueError): gate.helper.replay(gate.model.StatePreparation(control['fault']),trace,control['property'])
    def test_exact_named_witness_is_required(self):
        spec=gate.model.StatePreparation();report,graph=gate.helper.explore(spec,100000)
        states,edges,parents=graph;edges=[[(a,d) for a,d in row if a!='SelectCase-0'] for row in edges]
        original=gate.helper.explore
        with patch.object(gate.helper,'explore',lambda model,cap:(report,(states,edges,parents)) if model.fault is None else original(model,cap)):
            with self.assertRaises(AssertionError): gate.run()
    def test_loaded_sources_are_captured(self):
        path=str(Path(gate.__file__).resolve());self.assertIn(path,gate.helper.LOADED_SOURCES)
        with patch.dict(gate.helper.LOADED_SOURCES,{path:'0'*64}):
            with self.assertRaises(ValueError): gate.helper.validate_loaded_sources()
    def test_failed_rerun_and_bad_cli_replace_pass(self):
        root=Path(__file__).resolve().parents[2];output=root/'target/api007-formal/test-state-preparation-rerun.json'
        command=[sys.executable,str(root/'scripts/formal/check_state_preparation.py'),'--output',str(output)]
        try:
            for suffix,status in [([],0),(['--max-states','1'],1),([],0),(['--max-states','bad'],1),([],0),(['--out',str(output)],1)]:
                self.assertEqual(subprocess.run(command+suffix,capture_output=True).returncode,status)
                self.assertEqual(json.loads(output.read_text())['status'],'PASS' if status==0 else 'FAIL')
        finally: output.unlink(missing_ok=True)
    def test_startup_unavailable_source_replaces_pass(self):
        root=Path(__file__).resolve().parents[2];output=root/'target/api007-formal/test-state-preparation-startup.json';read=Path.read_bytes
        try:
            self.assertEqual(bootstrap.main(['--output',str(output)]),0)
            def unavailable(path):
                if path.name=='state_preparation_models.py': raise FileNotFoundError('injected missing model')
                return read(path)
            with patch.object(Path,'read_bytes',unavailable): self.assertEqual(bootstrap.main(['--output',str(output)]),1)
            self.assertEqual(json.loads(output.read_text())['status'],'FAIL')
        finally: output.unlink(missing_ok=True)
