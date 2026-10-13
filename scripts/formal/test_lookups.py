"""Finite lookup graph controls and captured fail-closed evidence runner."""
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch
import check_lookups as bootstrap
gate = bootstrap.load_runtime()

class LookupGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.report = gate.run()
    def test_complete_graph_and_replayable_mutants(self):
        report = self.report['baseline'][0]
        self.assertTrue(report['complete'])
        self.assertGreater(report['states'],1)
        self.assertEqual(len(report['completed_witnesses']),7)
        self.assertEqual(len(self.report['negative_controls']),5)
        for control in self.report['negative_controls']:
            gate.helper.replay(gate.model.Lookups(control['fault']),control['trace'],control['property'])
    def test_independent_scope_oracle_includes_empty_and_aliases(self):
        self.assertEqual(gate.model.Lookups.expected(((),(),0,0,0)),())
        self.assertIsNone(gate.model.Lookups.expected(((),(),0,1,0)))
        self.assertEqual(gate.model.Lookups.expected(((0,1,0),(0,1,0),1,0,0)),(0,None,0))
        self.assertIsNone(gate.model.Lookups.expected(((0,1),(0,),3,0,0)))
    def test_incomplete_search_and_damaged_trace_fail(self):
        with self.assertRaises(gate.helper.SearchLimit): gate.run(1)
        control=self.report['negative_controls'][0]
        trace=json.loads(json.dumps(control['trace']));trace[-1]['state']['accepted']=not trace[-1]['state']['accepted']
        with self.assertRaises(ValueError): gate.helper.replay(gate.model.Lookups(control['fault']),trace,control['property'])
    def test_exact_named_witness_is_required(self):
        spec=gate.model.Lookups();report,graph=gate.helper.explore(spec,100000)
        states,edges,parents=graph;edges=[[(a,d) for a,d in row if a!='SelectCase-0'] for row in edges]
        original=gate.helper.explore
        with patch.object(gate.helper,'explore',lambda model,cap:(report,(states,edges,parents)) if model.fault is None else original(model,cap)):
            with self.assertRaises(AssertionError): gate.run()
    def test_loaded_sources_are_captured(self):
        path=str(Path(gate.__file__).resolve());self.assertIn(path,gate.helper.LOADED_SOURCES)
        with patch.dict(gate.helper.LOADED_SOURCES,{path:'0'*64}):
            with self.assertRaises(ValueError): gate.helper.validate_loaded_sources()
    def test_failed_rerun_and_bad_cli_replace_pass(self):
        root=Path(__file__).resolve().parents[2];output=root/'target/api007-formal/test-lookup-rerun.json'
        command=[sys.executable,str(root/'scripts/formal/check_lookups.py'),'--output',str(output)]
        try:
            for suffix,status in [([],0),(['--max-states','1'],1),([],0),(['--max-states','bad'],1),([],0),(['--out',str(output)],1)]:
                self.assertEqual(subprocess.run(command+suffix,capture_output=True).returncode,status)
                self.assertEqual(json.loads(output.read_text())['status'],'PASS' if status==0 else 'FAIL')
        finally: output.unlink(missing_ok=True)
    def test_startup_unavailable_source_replaces_pass(self):
        root=Path(__file__).resolve().parents[2];output=root/'target/api007-formal/test-lookup-startup.json';read=Path.read_bytes
        try:
            self.assertEqual(bootstrap.main(['--output',str(output)]),0)
            def unavailable(path):
                if path.name=='lookup_models.py': raise FileNotFoundError('injected missing model')
                return read(path)
            with patch.object(Path,'read_bytes',unavailable): self.assertEqual(bootstrap.main(['--output',str(output)]),1)
            self.assertEqual(json.loads(output.read_text())['status'],'FAIL')
        finally: output.unlink(missing_ok=True)
