"""Independent batch witnesses, prefix forgeries and seventh-campaign failure gates."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import check_source_group as bootstrap

gate=bootstrap.load_runtime()
model=gate.batch_model
oracle=gate.batch_gate

class SourceBatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.report,cls.controls=oracle.run_batch_campaign(gate.helper,model,100000)
    def terminal(self,name):
        spec=model.SourceBatchComposition()
        s=next(s for _,s in spec.transitions(spec.initial) if s.case==name)
        while s.phase!='done':
            before=s;s=list(spec.transitions(s))[0][1]
            self.assertIsNone(spec.violation(before,'Renamed',s),name)
        return spec,s
    def test_complete_seventh_identity_exact_witnesses_and_replayed_mutants(self):
        self.assertEqual(self.report['name'],'api007-source-batch-composition')
        self.assertTrue(self.report['complete'])
        self.assertEqual(len(model.BATCH_CASES),21)
        self.assertEqual(len(model.BATCH_FAULTS),30)
        self.assertEqual(set(self.report['completed_witnesses']),set(model.BATCH_CASES))
        self.assertEqual({c['fault'] for c in self.controls},set(model.BATCH_FAULTS))
        for c in self.controls:
            gate.helper.replay(model.SourceBatchComposition(c['fault']),c['trace'],c['property'])
    def test_independent_exact_contents_reject_deleted_events_and_forged_fields(self):
        for name,c in model.BATCH_CASES.items():
            spec,s=self.terminal(name)
            self.assertTrue(oracle.completed_batch_witness(s,c,model),name)
            for i in range(len(s.events)):
                forged=replace(s,events=s.events[:i]+s.events[i+1:])
                self.assertFalse(oracle.completed_batch_witness(forged,c,model),(name,i))
                self.assertIsNotNone(spec.violation(spec.initial,'ErasedEvent',forged),(name,i))
            for changes in ({'before':model.DELETED_A},{'image':replace(s.image,sequence=s.image.sequence+1)},
                {'plan_live':True},{'output_live':True},{'output_credit':True},
                {'binding':('B','D','P','J')},{'acknowledged':True},
                {'fields':s.fields+(('w',1,'foreign'),)},{'candidate':s.candidate+1}):
                self.assertFalse(oracle.completed_batch_witness(replace(s,**changes),c,model),(name,changes))
            if s.returned:
                image,positions,receipts,binding=s.returned
                for output in ((model.INITIAL,positions,receipts,binding),
                    (image,positions+(99,),receipts,binding),(image,positions,(),binding),
                    (image,positions,receipts,('A','E','P','J'))):
                    self.assertFalse(oracle.completed_batch_witness(replace(s,returned=output),c,model),(name,output))
    def test_packing_controls_cover_full_first_prefix_outer_limits_and_sequence(self):
        _,full=self.terminal('nonmonotonic_full_run')
        self.assertEqual(full.outcomes,(('committed',2),('committed',3)))
        self.assertEqual(full.attempts,(2,))
        self.assertEqual(full.retained,2)
        self.assertIsNone(full.returned[0].work[0])
        self.assertEqual(full.returned[0].highwater[0],3)
        _,prefix=self.terminal('physical_prefix')
        self.assertEqual(prefix.attempts,(2,1))
        self.assertEqual(prefix.retained,1)
        self.assertEqual(prefix.returned[0],model.LARGE_A)
        self.assertEqual(prefix.returned[1],(0,))
        self.assertEqual(prefix.returned[2],(('z',('committed',2)),))
        _,offset=self.terminal('caller_position_offset')
        self.assertEqual(offset.returned[1],(2,3))
        self.assertEqual(offset.returned[2],(('a',('committed',2)),('z',('committed',2))))
        _,exact=self.terminal('outer_exact');_,short=self.terminal('outer_one_short')
        self.assertEqual((exact.result,short.result),('sealed','too-large'))
        _,recreated=self.terminal('work_delete_recreate_sequence')
        self.assertEqual(recreated.returned[0].sequence,3)
        self.assertEqual(recreated.returned[0].highwater[0],3)
    def test_invalid_prefixes_remain_invalid_after_later_unknown(self):
        spec=model.SourceBatchComposition()
        s=next(s for _,s in spec.transitions(spec.initial) if s.case=='per_identity_fields_and_late_probe')
        while s.phase!='probe':s=list(spec.transitions(s))[0][1]
        for changes in ({'fields':s.fields[:-1]},{'before':model.DELETED_A},
            {'image':model.PATCHED_A},{'plan_live':False},{'binding':('B','D','P','J')},
            {'loaded':(1,0)},{'clocks':(0,)},{'events':(('fields',-1),)}):
            bad=replace(s,**changes)
            self.assertIsNotNone(spec.violation(spec.initial,'ExposeBadPrefix',bad),changes)
            late=replace(bad,phase='done',result='unknown',plan_live=False,events=bad.events+(('unknown',-1),))
            self.assertIsNotNone(spec.violation(spec.initial,'LaterUnknown',late),changes)
    def test_omitted_required_probe_is_rejected_before_and_after_unknown(self):
        spec=model.SourceBatchComposition()
        s=next(s for _,s in spec.transitions(spec.initial) if s.case=='per_identity_fields_and_late_probe')
        while not (s.phase=='clock' and s.index==1):s=list(spec.transitions(s))[0][1]
        self.assertEqual(s.loaded,(1,))
        forged=replace(s,loaded=(),owners=(None,None))
        self.assertEqual(spec.violation(spec.initial,'OmitRequiredRead',forged),'SB-OVERLAY')
        late=replace(forged,phase='done',result='unknown',plan_live=False,events=forged.events+(('unknown',-1),))
        self.assertEqual(spec.violation(spec.initial,'LaterUnknown',late),'SB-OVERLAY')
    def test_encode_moved_before_prior_admission_cannot_use_future_plans(self):
        spec,s=self.terminal('release_then_transfer')
        encode=next(event for event in s.events if event[0]=='encode')
        moved=(encode,)+tuple(event for event in s.events if event!=encode)
        forged=replace(s,events=moved)
        self.assertEqual(spec.violation(spec.initial,'Renamed',forged),'SB-PACK')
        self.assertFalse(oracle.completed_batch_witness(forged,model.BATCH_CASES[s.case],model))
        late=replace(forged,phase='done',result='unknown',returned=(),retained=0,best=0,
            plan_live=False,output_live=False,output_credit=False,
            events=tuple(event for event in moved if event[0] not in ('seal','borrow','drop'))+(('unknown',-1),))
        self.assertEqual(spec.violation(spec.initial,'LaterUnknown',late),'SB-PACK')
    def test_foreign_seal_followed_by_unknown_cannot_retain_or_hide_output(self):
        spec=model.SourceBatchComposition()
        s=next(s for _,s in spec.transitions(spec.initial) if s.case=='foreign_equal_head_cut')
        while s.phase!='bind':s=list(spec.transitions(s))[0][1]
        forged=replace(s,phase='done',result='unknown',plan_live=False,
            output_live=True,output_credit=True,returned=(),
            events=s.events+(('seal',s.retained),('unknown',-1)))
        self.assertEqual(spec.violation(spec.initial,'Renamed',forged),'SB-BIND')
        for changes in ({'output_live':False,'output_credit':False},
            {'case':'per_identity_fields_and_late_probe'}):
            self.assertIsNotNone(spec.violation(spec.initial,'LaterUnknown',replace(forged,**changes)))
    def test_seal_counter_and_phase_reset_cannot_hide_output_retirement(self):
        spec=model.SourceBatchComposition()
        s=next(s for _,s in spec.transitions(spec.initial) if s.case=='release_then_transfer')
        while s.phase!='borrow':s=list(spec.transitions(s))[0][1]
        forged=replace(s,retained=0,returned=(),result='',phase='probe',
            plan_live=True,output_live=False,output_credit=False)
        self.assertEqual(spec.violation(spec.initial,'RenamedPhase',forged),'SB-PACK')
        kept_counter=replace(forged,retained=s.retained)
        self.assertIsNotNone(spec.violation(spec.initial,'HideOwnerOnly',kept_counter))
    def test_wrong_drop_index_and_terminal_live_owners_are_rejected(self):
        spec=model.SourceBatchComposition()
        s=next(s for _,s in spec.transitions(spec.initial) if s.case=='release_then_transfer')
        while s.phase!='drop':s=list(spec.transitions(s))[0][1]
        forged=replace(s,phase='done',events=s.events+(('drop',0),))
        self.assertEqual(spec.violation(spec.initial,'WrongIndex',forged),'SB-PREFIX')
        correct_index=replace(forged,events=s.events+(('drop',-1),))
        self.assertEqual(spec.violation(spec.initial,'RetainDroppedOwner',correct_index),'SB-OWNER')
    def test_forged_success_for_every_failure_fixture_is_rejected(self):
        for name,c in model.BATCH_CASES.items():
            if oracle.expected_batch(c,model)['result']=='sealed':continue
            normal=replace(c,response_ok=True,admission=True,output_admission=True,
                source_failure='',frame=64,record=66,requested_binding=('A','D','P','J'))
            forged=model.BatchState(case=name,**oracle.expected_batch(normal,model))
            spec=model.SourceBatchComposition()
            self.assertIsNotNone(spec.violation(spec.initial,'InventSuccessfulPort',forged),name)
            self.assertFalse(oracle.completed_batch_witness(forged,c,model),name)
    def test_literal_ghost_fixture_drift_is_rejected_by_external_relational_oracle(self):
        c=model.BATCH_CASES['release_then_transfer']
        broken=replace(c,expected=((model.INITIAL,('committed',2)),)+c.expected[1:])
        with self.assertRaisesRegex(AssertionError,'literal batch fixtures'):
            oracle.expected_batch(broken,model)
    def fail_closed(self,context,marker):
        out=Path(__file__).resolve().parents[2]/'target/api007-formal/test-source-batch-failure.json'
        try:
            out.parent.mkdir(parents=True,exist_ok=True);out.write_text('{"status":"PASS"}')
            with context(),patch.object(bootstrap,'load_runtime',lambda:gate):
                self.assertEqual(bootstrap.main(['--output',str(out)]),1)
            failure=json.loads(out.read_text())
            self.assertEqual(failure['status'],'FAIL');self.assertIn(marker,failure['error'])
        finally:out.unlink(missing_ok=True)
    def test_missing_seventh_campaign_cannot_leave_a_six_campaign_pass(self):
        self.fail_closed(lambda:patch.object(model.SourceBatchComposition,'name','omitted-batch'),
            'seven exact source campaign identities required')
    def test_only_seventh_search_limit_and_runtime_error_replace_stale_pass(self):
        original=gate.helper.explore
        def limited(spec,cap):
            return original(spec,1 if spec.name=='api007-source-batch-composition' and spec.fault is None else cap)
        self.fail_closed(lambda:patch.object(gate.helper,'explore',limited),
            'api007-source-batch-composition: incomplete search at 1 states')
        def broken(spec,cap):
            if spec.name=='api007-source-batch-composition':raise RuntimeError('injected batch exploration runtime')
            return original(spec,cap)
        self.fail_closed(lambda:patch.object(gate.helper,'explore',broken),'injected batch exploration runtime')
    def test_missing_completed_batch_history_cannot_be_hidden_by_green_baseline(self):
        spec=model.SourceBatchComposition();report,graph=gate.helper.explore(spec,100000)
        states,edges,parents=graph
        damaged=[[edge for edge in row if edge[0]!='SelectBatch-nonmonotonic_full_run'] for row in edges]
        original=gate.helper.explore
        def omitted(candidate,cap):
            if candidate.name==spec.name and candidate.fault is None:return report,(states,damaged,parents)
            return original(candidate,cap)
        self.fail_closed(lambda:patch.object(gate.helper,'explore',omitted),
            "missing completed witness: ['SelectBatch-nonmonotonic_full_run']")
    def test_executed_batch_files_are_captured_and_must_match_provenance(self):
        for name in ('source_batch_models.py','source_batch_gate.py'):
            path=Path(__file__).with_name(name).resolve()
            self.assertEqual(gate.helper.LOADED_SOURCES[str(path)],hashlib.sha256(path.read_bytes()).hexdigest())
            self.fail_closed(lambda path=path:patch.dict(gate.helper.LOADED_SOURCES,{str(path):'0'*64}),
                'loaded executable source differs from provenance: '+str(path))
    def test_missing_syntax_and_runtime_failure_of_batch_modules_invalidates_pass(self):
        out=Path(__file__).resolve().parents[2]/'target/api007-formal/test-source-batch-module-failure.json'
        read=Path.read_bytes
        try:
            for name in ('source_batch_models.py','source_batch_gate.py'):
                for mode in ('missing','syntax','runtime'):
                    marker='injected batch '+mode
                    def altered(path,name=name,mode=mode,marker=marker):
                        if path.name!=name:return read(path)
                        if mode=='missing':raise FileNotFoundError(marker)
                        return b'invalid ! syntax' if mode=='syntax' else ('raise RuntimeError('+repr(marker)+')').encode()
                    out.parent.mkdir(parents=True,exist_ok=True);out.write_text('{"status":"PASS"}')
                    with patch.object(Path,'read_bytes',altered):
                        self.assertEqual(bootstrap.main(['--output',str(out)]),1)
                    self.assertEqual(json.loads(out.read_text())['status'],'FAIL')
        finally:out.unlink(missing_ok=True)
