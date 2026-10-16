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
        self.assertEqual({c['fault'] for c in self.report['negative_controls']}, set(gate.model.FAULTS) | set(gate.model.PUBLICATION_FAULTS) | set(gate.model.READ_FAULTS) | set(gate.model.PROVISION_FAULTS) | set(gate.model.HEADER_FAULTS) | set(gate.original_model.ORIGINAL_FAULTS) | set(gate.batch_model.BATCH_FAULTS) | set(gate.original_batch_model.FAULTS) | set(gate.coordinator_model.FAULTS) | set(gate.body_model.FAULTS) | set(gate.state_read_model.FAULTS) | set(gate.selection_model.FAULTS))
        for control in self.report['negative_controls']:
            spec = (gate.model.SourceGroup if control['fault'] in gate.model.FAULTS else
                    gate.model.SourceGroupPublication if control['fault'] in gate.model.PUBLICATION_FAULTS
                    else gate.model.SourceRecordRead if control['fault'] in gate.model.READ_FAULTS
                    else gate.model.CoupledGenesisProvision if control['fault'] in gate.model.PROVISION_FAULTS
                    else gate.model.SourceHeaderRecovery if control['fault'] in gate.model.HEADER_FAULTS
                    else gate.original_model.SourceOriginalResolution if control['fault'] in gate.original_model.ORIGINAL_FAULTS
                    else gate.batch_model.SourceBatchComposition if control['fault'] in gate.batch_model.BATCH_FAULTS
                    else gate.original_batch_model.SourceOriginalBatch if control['fault'] in gate.original_batch_model.FAULTS
                    else gate.coordinator_model.Coordinator if control['fault'] in gate.coordinator_model.FAULTS
                    else gate.body_model.SourceBodies if control['fault'] in gate.body_model.FAULTS
                    else gate.state_read_model.SourceStateRead if control['fault'] in gate.state_read_model.FAULTS
                    else gate.selection_model.SourceSelection)
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

    def test_record_shared_ledger_and_retained_alias_witnesses(self):
        from dataclasses import replace
        report = self.report['baseline'][2]
        self.assertTrue(report['complete'])
        self.assertEqual(set(report['completed_witnesses']), set(gate.READ_WITNESSES))
        for name, witness in report['completed_witnesses'].items():
            state = witness['trace'][-1]['state']
            self.assertEqual((state['phase'], state['credit'], state['semantic']), ('done', 0, 7))
            events = [tuple(event) for event in state['events']]
            if name in ('reads_one_short', 'bytes_one_short'):
                self.assertNotIn(('grant', 0), events)
                self.assertNotIn(('grant', 1), events)
            else:
                self.assertIn(('plan', 0), events)
                self.assertIn(('plan', 1), events)
                self.assertIn(('alias', 1), events)
            if name == 'cancel_then_retry':
                pending = next(row['state'] for row in witness['trace']
                    if row['action'] == 'CancelCallerPhysicalStillRunning')
                self.assertEqual((pending['credit'], pending['attempts'], pending['reads'], pending['body']),
                    (9, 1, 2, 5))
            if name == 'retained_aliases':
                retained = next(row['state'] for row in reversed(witness['trace'])
                    if row['action'] == 'PhysicalRetiresAliasSurvives')
                self.assertEqual(retained['credit'], 18)
        for name, witness in report['completed_witnesses'].items():
            row = witness['trace'][-1]['state']
            state = gate.model.ReadState(**{**row, 'case':tuple(row['case']),
                'events':tuple(tuple(event) for event in row['events'])})
            case = gate.READ_WITNESSES[name]
            self.assertTrue(gate.completed_record_witness(state, case))
            for i in range(len(state.events)):
                self.assertFalse(gate.completed_record_witness(
                    replace(state, events=state.events[:i]+state.events[i+1:]), case))
        model = gate.model.SourceRecordRead()
        state = gate.model.ReadState(case=('same',2,4,10,'success'), phase='next',
            attempts=1, reads=2, body=5, credit=9,
            events=(('plan',0),('grant',0),('fund',0),('hydrate',0),('retire',0),('alias',0)))
        self.assertIsNone(model.violation(state, 'Renamed', state))
        self.assertEqual(model.violation(state, 'Renamed', replace(state, phase='done')), 'R-OWNER')
        empty_done = gate.model.ReadState(case=('same',2,4,10,'success'),
            phase='done', attempts=2, reads=4, body=10)
        self.assertEqual(model.violation(state, 'Renamed', empty_done), 'R-OWNER')
        forged_alias = gate.model.ReadState(case=('same',2,4,10,'success'),
            attempts=2, reads=4, body=10, events=(('alias',0),))
        self.assertEqual(model.violation(state, 'Renamed', forged_alias), 'R-AUTHORITY')
        pending = replace(state, events=state.events[:4])
        self.assertIsNone(model.violation(state, 'Renamed', pending))
        early_release = replace(pending, credit=0, events=pending.events+(('release',0),))
        self.assertEqual(model.violation(pending, 'Renamed', early_release), 'R-OWNER')
        live_alias_release = replace(state, credit=0, events=state.events+(('release',0),))
        self.assertEqual(model.violation(state, 'Renamed', live_alias_release), 'R-OWNER')
        for missing in range(5):
            forged = replace(state, events=state.events[:missing]+state.events[missing+1:])
            self.assertIsNotNone(model.violation(state, 'Renamed', forged))
        for changes, expected in (({'attempts':2},'R-WORK'), ({'reads':4},'R-WORK'),
                ({'body':10},'R-WORK'), ({'credit':3},'R-OWNER'),
                ({'semantic':8},'R-SEMANTIC'), ({'acknowledged':True},'R-ACK'),
                ({'case':('same',2,4,10,'malformed')},'R-AUTHORITY')):
            self.assertEqual(model.violation(state, 'Renamed', replace(state, **changes)), expected)

    def test_genesis_history_oracle_witnesses_and_forged_success(self):
        from dataclasses import replace
        report = self.report['baseline'][3]
        self.assertTrue(report['complete'])
        self.assertEqual(set(report['completed_witnesses']),set(gate.model.PROVISION_CASES))
        model = gate.model.CoupledGenesisProvision()
        for name,case in gate.model.PROVISION_CASES.items():
            state = next(s for _,s in model.transitions(model.initial) if s.case == name)
            while state.phase != 'done': state = list(model.transitions(state))[0][1]
            self.assertTrue(gate.completed_genesis_witness(state,case),name)
            self.assertIsNone(model.violation(state,'Renamed',state))
            for i in range(len(state.events)):
                self.assertFalse(gate.completed_genesis_witness(replace(state,
                    events=state.events[:i]+state.events[i+1:]),case),(name,i))
            self.assertFalse(gate.completed_genesis_witness(replace(state,
                events=state.events+('open',)),case),name)
            if 'possible' in state.events:
                self.assertEqual(model.violation(state,'Renamed',replace(state,
                    possible_submit=False,original_live=False)),'GP-ORIGINAL')
            if name == 'known_missing':
                forged = replace(state,history=(gate.model.GENESIS_A,),remaining=1,fences=1,
                    possible_submit=True,original_live=True,
                    events=('preflight','open','empty','acquire','grant-write','possible','cas','reply-lost'))
                self.assertEqual(model.violation(state,'Renamed',forged),'GP-INITIAL')
            if name == 'delayed_commit_after_empty':
                trace = report['completed_witnesses'][name]['trace']
                returned = next(row['state'] for row in trace if row['action'] == 'ReturnUnknownBeforePhysicalSettlement')
                self.assertEqual((returned['result'],returned['phase'],returned['history']),('unknown','settling',()))
                early = replace(state,phase='capture',history=(),
                    events=state.events[:state.events.index('capture')]+('return-unknown',))
                self.assertEqual(model.violation(state,'Renamed',early),'GP-ORIGINAL')
            if name == 'equivalent_encoding_other_secret':
                forged = replace(state,fences=1,events=('preflight','open','empty','acquire')+state.events[2:])
                self.assertEqual(model.violation(state,'Renamed',forged),'GP-INITIAL')
            if name == 'own_lost_reply':
                early = replace(state,phase='capture',original_live=False,remaining=1,
                    result='unknown',returned=(),snapshot=(),
                    events=state.events[:state.events.index('capture')]+('return-unknown',))
                self.assertEqual(model.violation(state,'Renamed',early),'GP-ORIGINAL')
                self.assertIn('reply-lost',state.events)
                self.assertEqual(state.history[0],gate.model.GENESIS_A)
            if name == 'delayed_commit_after_empty':
                self.assertEqual((state.result,state.snapshot,state.history),
                    ('unknown',(),(gate.model.GENESIS_A,)))
            if state.result == 'success':
                for event in ('capture','grant-read','point','record','verified'):
                    events = list(state.events); events.remove(event)
                    forged = replace(state,events=tuple(events))
                    self.assertIsNotNone(model.violation(state,'Renamed',forged),(name,event))
                for changes in ({'proof':False},{'full_span':False},{'full_key':False},
                    {'stored_pin':'forged'},{'point_cut':'foreign'},{'snapshot':()}):
                    self.assertIsNotNone(model.violation(state,'Renamed',replace(state,**changes)),changes)
        blank = gate.model.ProvisionState(case='equivalent_encoding_other_secret',phase='done',
            history=(gate.model.GENESIS_B,),snapshot=(gate.model.GENESIS_B,),remaining=2,
            result='success',returned=gate.model.GENESIS_B)
        self.assertIsNotNone(model.violation(blank,'Renamed',blank))
        bad_admission = gate.model.ProvisionState(case='foreign_job',phase='done',remaining=2,
            events=('preflight','open'),result='unknown')
        self.assertEqual(model.violation(blank,'Renamed',bad_admission),'GP-ADMISSION')

    def test_header_recovery_independent_witnesses_and_each_prefix_prerequisite(self):
        from dataclasses import replace
        report=self.report['baseline'][4]
        self.assertTrue(report['complete'])
        self.assertEqual(set(report['completed_witnesses']),set(gate.model.HEADER_CASES))
        model=gate.model.SourceHeaderRecovery()
        for name,c in gate.model.HEADER_CASES.items():
            state=next(s for _,s in model.transitions(model.initial) if s.case==name)
            while state.phase!='done': state=list(model.transitions(state))[0][1]
            self.assertTrue(gate.completed_header_witness(state,c),name)
            self.assertIsNone(model.violation(state,'Renamed',state))
            for i in range(len(state.events)):
                forged=replace(state,events=state.events[:i]+state.events[i+1:])
                self.assertFalse(gate.completed_header_witness(forged,c),(name,i))
            # The terminal witness also rejects forged observations/acceptance
            # flags on refusal paths, independent of the model invariant.
            for field, value in (('genesis_cut','forged'),('sequence_cut','forged'),
                    ('genesis_key','forged'),('sequence_key','forged'),
                    ('genesis_verified',not state.genesis_verified),
                    ('sequence_verified',not state.sequence_verified),
                    ('correspondence',not state.correspondence),('record_epoch',99),
                    ('record_cut','forged'),('definition','forged'),('pin','forged')):
                self.assertFalse(gate.completed_header_witness(replace(state,**{field:value}),c),(name,field))
            if state.result=='capsule':
                for event in gate.HEADER_WORDS:
                    forged=replace(state,events=tuple(e for e in state.events if e!=event))
                    self.assertIsNotNone(model.violation(state,'UnrelatedLabel',forged),(name,event))
                for fields in ({'active':'foreign'}, {'ledger':'replacement'}, {'writer':'invented'},
                    {'record_cut':'foreign'}, {'genesis_key':'prefix'}, {'sequence_key':'prefix'},
                    {'record_epoch':99}, {'definition':'reencoded'}, {'source_live':False}):
                    forged=replace(state,**fields)
                    self.assertIsNotNone(model.violation(state,'Observe',forged),(name,fields))
        blank=gate.model.HeaderState(case='readonly',phase='done',result='capsule',active='A')
        self.assertIsNotNone(model.violation(model.initial,'ForgedSuccess',blank))

    def test_header_recovery_late_unknown_does_not_hide_invalid_accepted_evidence(self):
        from dataclasses import replace
        model=gate.model.SourceHeaderRecovery()
        controls={c['fault']:c for c in self.report['negative_controls']}
        for fault in gate.model.HEADER_FAULTS:
            control=controls[fault]
            self.assertEqual(control['status'],'COUNTEREXAMPLE')
        state=next(s for _,s in model.transitions(model.initial) if s.case=='readonly')
        while state.phase!='done': state=list(model.transitions(state))[0][1]
        forged=replace(state,sequence_cut='foreign',phase='done',result='unknown',capsule=(),
            source_live=False,record_live=False,definition_live=False)
        self.assertEqual(model.violation(state,'LaterUnknown',forged),'HR-CUT')
        for name,phase,fields,expected in (
                ('genesis_cut_aba','genesis-verify',{'genesis_cut':'A'},'HR-CUT'),
                ('sequence_cut_aba','sequence-verify',{'sequence_cut':'A'},'HR-CUT'),
                ('genesis_full_key','genesis-verify',{'genesis_key':'Genesis'},'HR-KEY'),
                ('sequence_full_key','sequence-verify',{'sequence_key':'Sequence'},'HR-KEY'),
                ('readonly','record-result',{'record_live':True},'HR-OWNER'),
                ):
            prefix=next(s for _,s in model.transitions(model.initial) if s.case==name)
            while prefix.phase!=phase: prefix=list(model.transitions(prefix))[0][1]
            self.assertEqual(model.violation(prefix,'ForgedObservation',replace(prefix,**fields)),expected)
        for fields,expected in (({'capsule':('foreign',)},'HR-OUTCOME'),
                ({'source_live':False},'HR-OWNER')):
            self.assertEqual(model.violation(state,'LaterUnknown',replace(state,phase='record-result',result='unknown',**fields)),expected)
        prefix=next(s for _,s in model.transitions(model.initial) if s.case=='readonly')
        self.assertEqual(model.violation(prefix,'EarlyRelease',replace(prefix,source_live=False)),'HR-OWNER')
        prefix=list(model.transitions(prefix))[0][1]
        for fields,expected in (({'case':''},'HR-PREFIX'),({'case':'acquired'},'HR-PREFIX'),
                ({'events':()},'HR-PREFIX'),({'semantic':prefix.semantic+1},'HR-WORK'),
                ({'records':prefix.records+1},'HR-WORK')):
            self.assertEqual(model.violation(prefix,'RelabeledRewind',replace(prefix,**fields)),expected)
        progressed=prefix
        while progressed.phase!='record-grant': progressed=list(model.transitions(progressed))[0][1]
        self.assertEqual(model.violation(progressed,'RelabeledRewind',prefix),'HR-WORK')
        for fields in ({'definition':'legacy'}, {'pin':'pin-legacy'}, {'capsule':('forged',)}):
            self.assertEqual(model.violation(prefix,'LaterUnknown',replace(prefix,**fields,result='unknown')),'HR-PREFIX')

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
