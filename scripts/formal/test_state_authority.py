"""Finite state-authority graph controls and captured fail-closed evidence runner."""
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch
import check_state_authority as bootstrap
gate = bootstrap.load_runtime()

class StateAuthorityGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.report = gate.run()
    def test_complete_graph_and_replayable_mutants(self):
        report = self.report['baseline'][0]
        self.assertTrue(report['complete'])
        self.assertGreater(report['states'],1)
        self.assertEqual(len(report['completed_witnesses']),13)
        self.assertEqual(len(self.report['negative_controls']),10)
        for control in self.report['negative_controls']:
            gate.helper.replay(gate.model.StateAuthority(control['fault']),control['trace'],control['property'])
    def test_allocation_order_control_requires_live_payload_fetch(self):
        control=next(row for row in self.report['negative_controls'] if row['fault']=='allocate_before_source_size')
        terminal=control['trace'][-1]
        self.assertEqual(terminal['action'],'FetchStatePayload')
        self.assertTrue(terminal['state']['fetched'])
        self.assertEqual(terminal['state']['case'][1],2)
        self.assertEqual(terminal['state']['case'][6],1)
        self.assertEqual(control['input_subset'],'intact live responses; final size check retained')
    def test_independent_scope_and_classification_oracle(self):
        self.assertEqual(gate.model.StateAuthority.expected((0,0,0,0,0,0,0)),('ok',()))
        self.assertEqual(gate.model.StateAuthority.expected((0,0,0,1,0,0,0)),('unknown',()))
        self.assertEqual(gate.model.StateAuthority.expected((6,2,0,0,0,0,1)),('too_large',()))
        self.assertEqual(gate.model.StateAuthority.expected((6,2,0,0,0,3,0)),('unknown',()))
    def test_incomplete_search_and_damaged_trace_fail(self):
        with self.assertRaises(gate.helper.SearchLimit): gate.run(1)
        control=self.report['negative_controls'][0]
        trace=json.loads(json.dumps(control['trace']));trace[-1]['state']['fetched']=not trace[-1]['state']['fetched']
        with self.assertRaises(ValueError): gate.helper.replay(gate.model.StateAuthority(control['fault']),trace,control['property'])
    def test_exact_named_witness_is_required(self):
        spec=gate.model.StateAuthority();report,graph=gate.helper.explore(spec,100000)
        states,edges,parents=graph;edges=[[(a,d) for a,d in row if a!='SelectCase-0'] for row in edges]
        original=gate.helper.explore
        with patch.object(gate.helper,'explore',lambda model,cap:(report,(states,edges,parents)) if model.fault is None else original(model,cap)):
            with self.assertRaises(AssertionError): gate.run()
    def test_loaded_sources_are_captured(self):
        path=str(Path(gate.__file__).resolve());self.assertIn(path,gate.helper.LOADED_SOURCES)
        with patch.dict(gate.helper.LOADED_SOURCES,{path:'0'*64}):
            with self.assertRaises(ValueError): gate.helper.validate_loaded_sources()
    def test_failed_rerun_and_bad_cli_replace_pass(self):
        root=Path(__file__).resolve().parents[2];output=root/'target/api007-formal/test-state-authority-rerun.json'
        command=[sys.executable,str(root/'scripts/formal/check_state_authority.py'),'--output',str(output)]
        try:
            for suffix,status in [([],0),(['--max-states','1'],1),([],0),(['--max-states','bad'],1),([],0),(['--out',str(output)],1)]:
                self.assertEqual(subprocess.run(command+suffix,capture_output=True).returncode,status)
                self.assertEqual(json.loads(output.read_text())['status'],'PASS' if status==0 else 'FAIL')
        finally: output.unlink(missing_ok=True)
    def test_startup_unavailable_source_replaces_pass(self):
        root=Path(__file__).resolve().parents[2];output=root/'target/api007-formal/test-state-authority-startup.json';read=Path.read_bytes
        try:
            self.assertEqual(bootstrap.main(['--output',str(output)]),0)
            def unavailable(path):
                if path.name=='state_authority_models.py': raise FileNotFoundError('injected missing model')
                return read(path)
            with patch.object(Path,'read_bytes',unavailable): self.assertEqual(bootstrap.main(['--output',str(output)]),1)
            self.assertEqual(json.loads(output.read_text())['status'],'FAIL')
        finally: output.unlink(missing_ok=True)
