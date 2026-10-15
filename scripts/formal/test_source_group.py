"""Independent source-group oracle, trace and fail-closed artifact controls."""
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import check_source_group as bootstrap

gate = bootstrap.load_runtime()

class SourceGroupGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.report = gate.run()
    def test_complete_graph_required_witnesses_and_replayed_mutants(self):
        baseline = self.report['baseline'][0]
        self.assertTrue(baseline['complete'])
        self.assertGreater(baseline['states'], len(gate.model.CASES))
        self.assertEqual(set(baseline['completed_witnesses']), set(gate.WITNESSES))
        self.assertEqual({c['fault'] for c in self.report['negative_controls']}, set(gate.model.FAULTS) | set(gate.model.PUBLICATION_FAULTS))
        for control in self.report['negative_controls']:
            spec = gate.model.SourceGroup if control['fault'] in gate.model.FAULTS else gate.model.SourceGroupPublication
            gate.helper.replay(spec(control['fault']), control['trace'], control['property'])
    def test_publication_boundary_witnesses_and_forged_preparation(self):
        from dataclasses import replace
        report = self.report['baseline'][1]
        self.assertTrue(report['complete'])
        self.assertEqual(len(report['completed_witnesses']), len(gate.model.PUBLICATION_CASES))
        model = gate.model.SourceGroupPublication()
        case = ('rejected', 'same', 'none')
        state = next(s for _, s in model.transitions(model.initial) if s.case == case)
        while state.phase != 'done': state = list(model.transitions(state))[0][1]
        self.assertTrue(state.prepared)
        self.assertIsNone(model.violation(state, 'Observe', state))
        blank = gate.model.PublicationState(case=case, phase='done', encoded=True,
            binding_verified=True, compiler_work=True, prepared=True)
        self.assertEqual(model.violation(model.initial, 'Forged', blank), 'P-PREFIX')
        for missing in ({'canonical_allocated':False}, {'frame_allocated':False},
            {'record_allocated':False}, {'canonical_admitted':False}, {'frame_admitted':False},
            {'record_admitted':False}, {'plan_live':False}, {'original_declarations':False},
            {'encoded':False}, {'binding_verified':False}, {'compiler_work':False}, {'acknowledged':True}):
            forged = replace(state, **missing)
            self.assertIsNotNone(model.violation(state, 'Renamed', forged), missing)

    def test_relational_oracle_and_planner_precedence(self):
        swap = gate.WITNESSES['unique_swap']
        self.assertEqual(gate.model.ghost_unique(swap), ('committed', ('B', 'A')))
        self.assertEqual(gate.model.planner_unique(swap, 'sequential_swap'), ('rejected', ()))
        self.assertEqual(gate.model.expected(gate.WITNESSES['missing_release_unknown']), 'unknown')
        self.assertEqual(gate.model.expected(gate.WITNESSES['business_rejection_precedes_release']), 'rejected')
        terminal = self.report['baseline'][0]['completed_witnesses']['lease_expires_during_probe']['trace'][-1]['state']
        self.assertEqual((terminal['clock'], terminal['sampled_time'], terminal['result']), (1, 2, 'rejected'))
    def test_unsafe_transition_is_not_hidden_by_later_unknown(self):
        controls = {c['fault']: c for c in self.report['negative_controls']}
        for fault in ('recapture_fields', 'read_check_fields', 'skip_source_bounds', 'validation_before_admission'):
            self.assertNotEqual(controls[fault]['trace'][-1]['state']['phase'], 'done')
        self.assertEqual(controls['recapture_fields']['trace'][-1]['state']['field_root'], 'root-1')
        self.assertFalse(controls['validation_before_admission']['trace'][-1]['state']['admitted'])
    def test_metadata_bound_oracle_is_independent_of_action_labels_and_terminal_classification(self):
        case = gate.WITNESSES['future_body']
        state = gate.model.State(case=case, phase='fields', fixed_read=True,
            metadata_accepted=True, key_bound=True, metadata_valid=False)
        model = gate.model.SourceGroup()
        for label in ('AcceptFixedMetadata', 'RenamedTransition', 'Observe'):
            self.assertEqual(model.violation(model.initial, label, state), 'G-BOUNDS')
        from dataclasses import replace
        terminal = replace(state, phase='done', result='unknown')
        self.assertEqual(model.violation(state, 'LaterUnknown', terminal), 'G-BOUNDS')
    def test_forged_success_and_each_missing_prefix_prerequisite_fail(self):
        from dataclasses import replace
        case = gate.WITNESSES['mixed_check_corruption_ignored']
        model = gate.model.SourceGroup()
        state = next(s for _, s in model.transitions(model.initial) if s.case == case)
        while state.phase != 'done': state = list(model.transitions(state))[0][1]
        self.assertIsNone(model.violation(state, 'Observe', state))
        invalid = [{'admitted':False}, {'bound':False}, {'fixed_read':False},
            {'metadata_accepted':False}, {'metadata_valid':False}, {'key_bound':False},
            {'fields_done':False}, {'fields':()}, {'unique_done':False}, {'clock':0},
            {'clock':2}, {'cancelled':True}, {'retargeted':True}, {'field_root':'root-1'}, {'sealed':False}]
        for missing in invalid:
            forged = replace(state, **missing)
            self.assertIsNotNone(model.violation(state, 'UnrelatedLabel', forged), missing)
        blank = gate.model.State(case=case, phase='done', result='committed', final_owners=gate.model.ghost_unique(case)[1])
        self.assertIsNotNone(model.violation(model.initial, 'ForgedSuccess', blank))
        unsafe_unknown = gate.model.State(case=case, phase='done', fixed_read=True, result='unknown')
        self.assertEqual(model.violation(model.initial, 'LaterUnknown', unsafe_unknown), 'G-PREFIX')
    def test_damaged_trace_and_incomplete_search_fail(self):
        with self.assertRaises(gate.helper.SearchLimit): gate.run(1)
        control = next(c for c in self.report['negative_controls'] if c['fault'] == 'recapture_fields')
        trace = json.loads(json.dumps(control['trace']))
        trace[-1]['state']['field_root'] = 'invented-root'
        with self.assertRaises(ValueError): gate.helper.replay(gate.model.SourceGroup(control['fault']), trace, control['property'])
    def test_required_positive_witness_cannot_be_removed(self):
        model = gate.model.SourceGroup()
        report, graph = gate.helper.explore(model, 100000)
        states, edges, parents = graph
        first = 'SelectCase-'+str(gate.model.CASES.index(gate.WITNESSES['unique_swap']))
        edges = [[(a,d) for a,d in row if a != first] for row in edges]
        original = gate.helper.explore
        with patch.object(gate.helper, 'explore', lambda spec, cap: (report, (states, edges, parents)) if spec.fault is None else original(spec, cap)):
            with self.assertRaises(AssertionError): gate.run()
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
    def test_captured_model_source_cannot_change(self):
        path = str(Path(gate.model.__file__).resolve())
        self.assertIn(path, gate.helper.LOADED_SOURCES)
        with patch.dict(gate.helper.LOADED_SOURCES, {path:'0'*64}):
            with self.assertRaises(ValueError): gate.helper.validate_loaded_sources()
