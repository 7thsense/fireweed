"""Independent batch-original witnesses, owner forgeries and eighth-campaign gates."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import check_source_group as bootstrap

gate=bootstrap.load_runtime()
model=gate.original_batch_model
oracle=gate.original_batch_gate

class SourceOriginalBatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.report,cls.controls=oracle.run_campaign(gate.helper,model,100000)
    def path(self,name):
        spec=model.SourceOriginalBatch()
        s=next(s for _,s in spec.transitions(spec.initial) if s.case==name)
        states=[s]
        while s.phase!='done':
            before=s;s=list(spec.transitions(s))[0][1]
            self.assertIsNone(spec.violation(before,'UnrelatedLabel',s),name)
            states.append(s)
        return spec,states
    def test_complete_eighth_identity_all_exact_witnesses_and_disjoint_replayed_mutants(self):
        self.assertEqual(self.report['name'],'api007-source-original-batch-resolution')
        self.assertTrue(self.report['complete'])
        self.assertEqual(set(self.report['completed_witnesses']),set(model.CASES))
        self.assertEqual({c['fault'] for c in self.controls},set(model.FAULTS))
        prior=[gate.model.FAULTS,gate.model.PUBLICATION_FAULTS,gate.model.READ_FAULTS,
            gate.model.PROVISION_FAULTS,gate.model.HEADER_FAULTS,gate.original_model.ORIGINAL_FAULTS,gate.batch_model.BATCH_FAULTS]
        self.assertFalse(set(model.FAULTS)&set().union(*prior))
        for c in self.controls: gate.helper.replay(model.SourceOriginalBatch(c['fault']),c['trace'],c['property'])
    def test_exact_terminal_witnesses_reject_deleted_events_and_forged_fields(self):
        for name,c in model.CASES.items():
            spec,path=self.path(name);s=path[-1]
            self.assertTrue(oracle.completed_witness(s,c),name)
            for i in range(len(s.events)):
                forged=replace(s,events=s.events[:i]+s.events[i+1:])
                self.assertFalse(oracle.completed_witness(forged,c),(name,i))
                self.assertIsNotNone(spec.violation(spec.initial,'EraseHistory',forged),(name,i))
            for fields in ({'cut':'foreign'},{'ledger':'other'},{'containers':True},
                {'pending':(9,)},{'frames':(1,)},{'dtos':(0,)},{'values':((0,('forged',)),)},
                {'points':s.points+1},{'reads':s.reads+1},{'classes':('forged',)},
                {'observations':s.observations+(('forged',),)},
                {'verified':s.verified+(99,)},{'returned':(('forged',),)},{'result':'forged'}):
                self.assertFalse(oracle.completed_witness(replace(s,**fields),c),(name,fields))
    def test_streaming_cancellation_retains_only_second_physical_owner_and_spent_work(self):
        spec,path=self.path('cancel_second')
        pending=next(s for s in path if s.pending==(2,) and s.phase=='read')
        self.assertEqual((pending.frames,pending.dtos,pending.verified),((),(0,),(0,)))
        self.assertEqual((pending.points,pending.reads),(2,2))
        cancelled=next(s for s in path if s.phase=='retire')
        self.assertEqual((cancelled.pending,cancelled.frames,cancelled.dtos,cancelled.containers),((2,),(),(),False))
        self.assertEqual((cancelled.points,cancelled.reads),(2,2))
        terminal=path[-1]
        self.assertEqual((terminal.pending,terminal.returned,terminal.result),((),(),'cancelled'))
        for fields in ({'pending':()},{'dtos':(0,)},{'containers':True},{'reads':1}):
            forged=replace(cancelled,**fields)
            self.assertIsNotNone(spec.violation(spec.initial,'ForgeCancelledOwner',forged),fields)
    def test_retained_mode_keeps_frame_one_until_cancel_then_retires_it(self):
        _,path=self.path('retained_cancel_second')
        pending=next(s for s in path if s.pending==(2,) and s.phase=='read')
        self.assertEqual((pending.frames,pending.dtos),((1,),()))
        cancelled=next(s for s in path if s.phase=='retire')
        self.assertEqual((cancelled.frames,cancelled.pending),((),(2,)))
    def test_callback_order_and_output_order_are_distinct_and_conflict_never_upgrades(self):
        _,path=self.path('mixed');s=path[-1]
        self.assertEqual([arg for word,arg in s.events if word=='convert'],[1,2,3,0])
        self.assertEqual([arg for word,arg in s.events if word=='dispatch'],[1,2])
        self.assertEqual([v[0] for v in s.returned],['present','conflict','absent','present'])
        _,path=self.path('absence_conflict')
        self.assertEqual(path[-1].reads,0)
        _,path=self.path('shared_frame_reverse')
        self.assertEqual(path[-1].reads,1)
        self.assertEqual([v[2] for v in path[-1].returned],[model.B.literal,model.A.literal])
    def test_all_fixed_facts_and_declarations_precede_dispatch_and_callbacks(self):
        for name in ('late_declaration_error','late_fixed_error','late_point_error'):
            _,path=self.path(name);s=path[-1]
            self.assertEqual(s.result,'unknown')
            self.assertFalse(any(word in ('dispatch','convert') for word,_ in s.events),name)
    def test_every_alias_checks_actual_receipt_not_only_agreement(self):
        for name in ('second_alias_start','second_alias_digest','second_alias_fingerprint',
            'second_alias_format','equally_wrong_aliases','same_bytes_other_identity',
            'wrong_checksum','wrong_length','late_frame_verification_error'):
            _,path=self.path(name)
            self.assertEqual((path[-1].result,path[-1].returned),('unknown',()),name)
        _,path=self.path('duplicate')
        self.assertEqual(path[-1].verified,(0,1))
        self.assertEqual(path[-1].reads,1)
    def test_bad_intermediate_evidence_cannot_be_hidden_by_later_unknown_or_phase_names(self):
        spec,path=self.path('reverse_offsets')
        s=next(s for s in path if s.phase=='convert' and s.member==0)
        for fields in ({'frames':()},{'containers':False},{'verified':()},
            {'cut':'B'},{'ledger':'other'},{'classes':('conflict','present')},
            {'points':0},{'events':s.events+(('return',-1),)}):
            bad=replace(s,**fields)
            self.assertIsNotNone(spec.violation(spec.initial,'Renamed',bad),fields)
            late=replace(bad,phase='done',events=bad.events+(('fail',-1),),result='unknown',
                containers=False,frames=(),dtos=(),values=())
            self.assertIsNotNone(spec.violation(spec.initial,'LaterUnknown',late),fields)
    def test_equal_bytes_cannot_exchange_verified_spans_with_identical_ids_and_events(self):
        spec,path=self.path('shared_frame_reverse')
        output=next(s for s in path if s.phase=='borrow')
        left,right=output.returned
        self.assertEqual(left[-1][0],right[-1][0])
        self.assertNotEqual(left[-1][1],right[-1][1])
        swapped=(left[:-1]+((left[-1][0],right[-1][1]),),
            right[:-1]+((right[-1][0],left[-1][1]),))
        self.assertEqual([v[:4] for v in swapped],[v[:4] for v in output.returned])
        forged=replace(output,returned=swapped)
        self.assertEqual(spec.violation(spec.initial,'SwapSpanOnly',forged),'BO-OUTPUT')
        # Clearing the output through later Unknown cannot erase the invalid
        # successful-return event or the missing explicit borrow/drop history.
        for returned in (swapped,()):
            late=replace(forged,phase='done',returned=returned,result='unknown',
                containers=False,dtos=(),values=(),events=forged.events+(('fail',-1),))
            self.assertIsNotNone(spec.violation(spec.initial,'LaterUnknown',late))
    def test_fixed_observations_remain_complete_for_present_and_conflict_only(self):
        spec,path=self.path('mixed')
        classified=next(s for s in path if s.phase=='nonpresent')
        self.assertIsNone(classified.observations[2])
        self.assertIsNotNone(classified.observations[1])
        for i in (0,1,3):
            for field in range(len(classified.observations[i])):
                changed=list(classified.observations[i]);changed[field]='foreign'
                observations=list(classified.observations);observations[i]=tuple(changed)
                forged=replace(classified,observations=tuple(observations))
                self.assertEqual(spec.violation(spec.initial,'ChangedFullStamp',forged),'BO-OUTPUT')
                late=replace(forged,phase='done',result='unknown',containers=False,
                    events=forged.events+(('fail',-1),))
                self.assertIsNotNone(spec.violation(spec.initial,'LaterUnknown',late))
        conflict=path[-1].returned[1]
        self.assertIsNotNone(conflict[3]);self.assertIsNone(conflict[4])
    def test_unselected_or_unknown_fixture_cannot_expose_output_owners_or_work(self):
        spec=model.SourceOriginalBatch()
        self.assertIsNone(spec.violation(spec.initial,'Observe',spec.initial))
        for fields in ({'phase':'done','result':'batch','returned':(('forged',),),'containers':True},
            {'containers':True},{'pending':(1,)},{'dtos':(0,)},{'points':1},
            {'observations':(('forged',),)},{'events':(('fail',-1),)},
            {'case':'unknown_fixture','phase':'done'}):
            forged=replace(spec.initial,**fields)
            self.assertIsNotNone(spec.violation(spec.initial,'Renamed',forged),fields)

    def test_late_read_and_nonpresent_conversion_errors_retire_prior_owners(self):
        for name in ('late_read_error','retained_late_read_error'):
            _,path=self.path(name)
            pending=next(s for s in path if s.phase=='read' and s.pending==(2,))
            self.assertEqual((pending.points,pending.reads),(2,2))
            if name=='late_read_error': self.assertEqual((pending.dtos,pending.frames),((0,),()))
            else: self.assertEqual((pending.dtos,pending.frames),((),(1,)))
            end=path[-1]
            self.assertEqual((end.result,end.returned,end.points,end.reads),('unknown',(),2,2))
            self.assertEqual((end.containers,end.pending,end.frames,end.dtos),(False,(),(),()))
        _,path=self.path('late_nonpresent_conversion_error')
        self.assertTrue(any(s.dtos==(0,) for s in path))
        end=path[-1]
        self.assertEqual((end.result,end.reads,end.dtos),('unknown',0,()))
    def test_genuine_late_unknown_accepts_valid_prefix_and_rejects_corrupt_evidence(self):
        spec,path=self.path('late_conversion_error')
        before,end=path[-2:]
        self.assertEqual(end.result,'unknown')
        self.assertTrue(before.dtos)
        self.assertIsNone(spec.violation(before,'ActualLateFailure',end))
        self.assertTrue(oracle.completed_witness(end,model.CASES[end.case]))
        changed=list(end.observations);stamp=list(changed[0]);stamp[6]+=1;changed[0]=tuple(stamp)
        for fields in ({'observations':tuple(changed)},{'verified':end.verified[:-1]},
            {'events':tuple(e for e in end.events if e!=('verify',0))},
            {'dtos':before.dtos},{'frames':before.frames},{'reads':end.reads-1}):
            forged=replace(end,**fields)
            self.assertIsNotNone(spec.violation(before,'ActualLateFailure',forged),fields)
            self.assertFalse(oracle.completed_witness(forged,model.CASES[end.case]),fields)

    def fail_closed(self,context,marker):
        out=Path(__file__).resolve().parents[2]/'target/api007-formal/test-source-original-batch-failure.json'
        try:
            out.parent.mkdir(parents=True,exist_ok=True);out.write_text('{"status":"PASS"}')
            with context(),patch.object(bootstrap,'load_runtime',lambda:gate):
                self.assertEqual(bootstrap.main(['--output',str(out)]),1)
            failure=json.loads(out.read_text());self.assertEqual(failure['status'],'FAIL')
            self.assertIn(marker,failure['error'])
        finally: out.unlink(missing_ok=True)
    def test_missing_eighth_campaign_cannot_leave_seven_campaign_success(self):
        self.fail_closed(lambda:patch.object(model.SourceOriginalBatch,'name','omitted-original-batch'),
            'eight exact source campaign identities required')
    def test_eighth_search_limit_and_runtime_error_invalidate_stale_pass(self):
        original=gate.helper.explore
        def limited(spec,cap):
            return original(spec,1 if spec.name=='api007-source-original-batch-resolution' and spec.fault is None else cap)
        self.fail_closed(lambda:patch.object(gate.helper,'explore',limited),
            'api007-source-original-batch-resolution: incomplete search at 1 states')
        def broken(spec,cap):
            if spec.name=='api007-source-original-batch-resolution':raise RuntimeError('injected original batch exploration runtime')
            return original(spec,cap)
        self.fail_closed(lambda:patch.object(gate.helper,'explore',broken),'injected original batch exploration runtime')
    def test_missing_required_cancellation_witness_cannot_hide_in_green_baseline(self):
        spec=model.SourceOriginalBatch();report,graph=gate.helper.explore(spec,100000)
        states,edges,parents=graph
        damaged=[[edge for edge in row if edge[0]!='ChooseOriginalBatch-cancel_second'] for row in edges]
        original=gate.helper.explore
        def omitted(candidate,cap):
            if candidate.name==spec.name and candidate.fault is None:return report,(states,damaged,parents)
            return original(candidate,cap)
        self.fail_closed(lambda:patch.object(gate.helper,'explore',omitted),
            "missing completed witness: ['ChooseOriginalBatch-cancel_second']")
    def test_executed_modules_are_captured_and_must_match_provenance(self):
        for name in ('source_original_batch_models.py','source_original_batch_gate.py'):
            path=Path(__file__).with_name(name).resolve()
            self.assertEqual(gate.helper.LOADED_SOURCES[str(path)],hashlib.sha256(path.read_bytes()).hexdigest())
            self.fail_closed(lambda path=path:patch.dict(gate.helper.LOADED_SOURCES,{str(path):'0'*64}),
                'loaded executable source differs from provenance: '+str(path))
    def test_missing_syntax_and_runtime_failures_of_each_module_invalidate_stale_pass(self):
        out=Path(__file__).resolve().parents[2]/'target/api007-formal/test-source-original-batch-module-failure.json'
        read=Path.read_bytes
        try:
            for name in ('source_original_batch_models.py','source_original_batch_gate.py'):
                for mode in ('missing','syntax','runtime'):
                    marker='injected original batch '+mode
                    def altered(path,name=name,mode=mode,marker=marker):
                        if path.name!=name:return read(path)
                        if mode=='missing':raise FileNotFoundError(marker)
                        return b'invalid ! syntax' if mode=='syntax' else ('raise RuntimeError('+repr(marker)+')').encode()
                    out.parent.mkdir(parents=True,exist_ok=True);out.write_text('{"status":"PASS"}')
                    with patch.object(Path,'read_bytes',altered):
                        self.assertEqual(bootstrap.main(['--output',str(out)]),1)
                    self.assertEqual(json.loads(out.read_text())['status'],'FAIL')
        finally:out.unlink(missing_ok=True)
