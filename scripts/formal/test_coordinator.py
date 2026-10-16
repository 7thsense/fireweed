"""Independent coordinator event, owner, completion and runner controls."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import check_source_group as bootstrap

gate=bootstrap.load_runtime()
model=gate.coordinator_model
oracle=gate.coordinator_gate
class CoordinatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.report,cls.controls=oracle.run_campaign(gate.helper,model,100000)
    def path(self,name):
        spec=model.Coordinator();s=next(s for _,s in spec.transitions(spec.initial) if s.case==name);states=[s]
        while s.phase!='done':
            before=s;s=list(spec.transitions(s))[0][1]
            self.assertIsNone(spec.violation(before,'renamed-action',s),name);states.append(s)
        return spec,states
    def test_all_fixture_witnesses_and_replayed_disjoint_mutants(self):
        self.assertEqual(set(self.report['completed_witnesses']),set(model.CASES))
        self.assertEqual({r['fault'] for r in self.controls},set(model.FAULTS))
        prior=[gate.model.FAULTS,gate.model.PUBLICATION_FAULTS,gate.model.READ_FAULTS,
            gate.model.PROVISION_FAULTS,gate.model.HEADER_FAULTS,gate.original_model.ORIGINAL_FAULTS,
            gate.batch_model.BATCH_FAULTS,gate.original_batch_model.FAULTS]
        self.assertFalse(set(model.FAULTS)&set().union(*prior))
        for row in self.controls:gate.helper.replay(model.Coordinator(row['fault']),row['trace'],row['property'])
    def test_every_prefix_and_deleted_event_is_checked_independent_of_phase_labels(self):
        for name,c in model.CASES.items():
            spec,path=self.path(name)
            self.assertTrue(oracle.completed_witness(path[-1],c))
            for state in path:
                for fields in ({'ledger':'other'},{'work':state.work+1},{'owner':not state.owner},
                    {'job':not state.job},{'caller':not state.caller},{'attempts':state.attempts+1},
                    {'declarations':('other',)},{'observed':state.observed+(('foreign',),)},
                    {'pending':state.pending+(99,)},{'output':(('early',),)}):
                    self.assertIsNotNone(spec.violation(spec.initial,'forged',replace(state,**fields)),(name,fields))
            final=path[-1]
            for p in range(len(final.events)):
                self.assertFalse(oracle.completed_witness(replace(final,events=final.events[:p]+final.events[p+1:]),c),(name,p))
    def test_ambiguity_requires_new_actual_acquire_and_complete_reclassification(self):
        _,path=self.path('ambiguous_absence_new_actual_acquire')
        released=next(s for s in path if s.events and s.events[-1][0]=='release')
        self.assertEqual(released.cut,3)
        self.assertTrue(model.CASES[released.case].cuts[released.cut].capability)
        self.assertGreater(model.CASES[released.case].cuts[released.cut].epoch,model.CASES[released.case].cuts[released.pending_cut].epoch)
        self.assertEqual(released.classes,('absent','absent'))
        self.assertEqual(sum(w=='submit' for w,_ in path[-1].events),2)
        for name in ('numeric_epoch_without_capability','same_epoch_actual_acquire_is_insufficient'):
            _,p=self.path(name);self.assertEqual(p[-1].result,'unknown');self.assertEqual(sum(w=='submit' for w,_ in p[-1].events),1)
    def test_durable_subset_failure_and_pre_vs_post_submit_errors(self):
        for name in ('stored_missing_second_original','durable_content_conflicting_second_original'):
            _,path=self.path(name);self.assertEqual(path[-1].result,'unknown')
            self.assertEqual(sum(w=='submit' for w,_ in path[-1].events),1)
        _,first=self.path('first_preparation_refusal');self.assertEqual(first[-1].result,'too-large')
        _,later=self.path('second_preparation_refusal_after_prefix');self.assertEqual(later[-1].result,'unknown')
        self.assertEqual(sum(w=='submit' for w,_ in later[-1].events),1)
    def test_abandonment_never_cancels_owned_work_and_shutdown_waits(self):
        _,path=self.path('abandoned_caller_owned_task_drains')
        waiting=next(s for s in path if s.events and s.events[-1][0]=='shutdown-pending')
        self.assertFalse(waiting.caller);self.assertTrue(waiting.job);self.assertTrue(waiting.owner)
        self.assertEqual(path[-1].result,'drained');self.assertEqual(path[-1].output,())
        self.assertFalse(path[-1].job);self.assertFalse(path[-1].owner)
    def test_partial_continuity_is_sticky_even_with_stale_confirmation(self):
        for name in ('partial_observation_sticky_despite_stale_head','known_presence_cannot_disappear'):
            _,path=self.path(name);sticky=next(s for s in path if s.sticky)
            self.assertEqual(sticky.observed[0],model.stamp(0,model.Fact()))
            self.assertFalse(path[-1].confirmed);self.assertEqual(path[-1].result,'unknown')
            self.assertFalse(any(w=='submit' for w,_ in path[-1].events))
    def test_progress_resets_contention_only_and_preserves_cumulative_work(self):
        _,path=self.path('two_prefixes_reset_only_confirmed_progress')
        resets=[(a,b) for a,b in zip(path,path[1:]) if b.attempts<a.attempts]
        self.assertEqual(len(resets),2)
        for before,after in resets:self.assertGreater(after.known,before.known);self.assertGreater(after.work,before.work)
        self.assertEqual(sum(w=='submit' for w,_ in path[-1].events),2)
        self.assertEqual([r[0] for r in next(s for s in path if s.phase=='returned').output],['z','a','m'])
    def test_receipt_content_cannot_be_substituted_under_identical_identity_and_stamp(self):
        spec,path=self.path('different_receipt_content_same_fingerprint')
        returned=next(s for s in path if s.phase=='returned')
        first,second=returned.output
        swapped=(first[:-1]+(second[-1],),second[:-1]+(first[-1],))
        self.assertEqual([r[:3] for r in swapped],[r[:3] for r in returned.output])
        for fields in ({'output':swapped},
            {'output':swapped,'events':returned.events[:-1]+(('output',swapped),)}):
            bad=replace(returned,**fields)
            self.assertEqual(spec.violation(spec.initial,'substitute-token',bad),'CO-OUTPUT')
        event_forgery=replace(path[-1],events=tuple(('output',swapped) if w=='output' else (w,a) for w,a in path[-1].events))
        self.assertEqual(spec.violation(spec.initial,'drop-cannot-erase-response',event_forgery),'CO-OUTPUT')
    def test_returned_dto_owns_admission_through_borrow_and_drop(self):
        spec,path=self.path('shared_prefix')
        for state in path:
            if state.phase in ('returned','drop-output'):
                self.assertTrue(state.output_owner);self.assertTrue(state.job);self.assertTrue(state.output)
                for fields in ({'output_owner':False},{'job':False},{'output':()}):
                    self.assertIsNotNone(spec.violation(spec.initial,'premature-return-release',replace(state,**fields)))
        self.assertFalse(path[-1].output_owner);self.assertFalse(path[-1].job);self.assertFalse(path[-1].output)
    def test_mixed_barriers_and_competitor_subset_have_exact_submissions(self):
        for name,expected in [('fresh_known_fresh_barrier',[(0,),(2,)]),
            ('ambiguous_prefix_competitor_resolves_subset',[(0,1),(1,)])]:
            _,path=self.path(name)
            self.assertEqual([a for w,a in path[-1].events if w=='prepare'],expected)
            self.assertEqual(sum(w=='submit' for w,_ in path[-1].events),2)
        returned=next(s for s in path if s.phase=='returned')
        self.assertEqual(returned.output[0][1],'conflict');self.assertEqual(returned.output[0][-1],'RequestConflict')
    def test_pending_failed_classification_never_permits_intermediate_acquire(self):
        for name in ('pending_late_fixed_error_cannot_acquire','pending_sticky_change_despite_stale_head'):
            _,path=self.path(name)
            self.assertEqual(path[-1].result,'unknown')
            submit=next(i for i,(w,_) in enumerate(path[-1].events) if w=='submit')
            self.assertFalse(any(w=='capture' and a[0]=='acquire' for w,a in path[-1].events[submit+1:]))
    def fail_closed(self,context,marker,reload=False):
        out=Path(__file__).resolve().parents[2]/'target/api007-formal/test-coordinator-failure.json'
        out.parent.mkdir(parents=True,exist_ok=True);out.write_text('{"status":"PASS"}')
        try:
            with context(),patch.object(bootstrap,'load_runtime',bootstrap.load_runtime if reload else lambda:gate):
                self.assertEqual(bootstrap.main(['--output',str(out)]),1)
            r=json.loads(out.read_text());self.assertEqual(r['status'],'FAIL');self.assertIn(marker,r['error'])
        finally:out.unlink(missing_ok=True)
    def test_ninth_identity_search_and_runtime_are_mandatory(self):
        self.fail_closed(lambda:patch.object(model.Coordinator,'name','omitted-coordinator'),'ninth exact source coordinator identity required')
        original=gate.helper.explore
        def limited(spec,cap):return original(spec,1 if spec.name==model.Coordinator.name and spec.fault is None else cap)
        self.fail_closed(lambda:patch.object(gate.helper,'explore',limited),'api007-native-commit-coordinator: incomplete search at 1 states')
        def broken(spec,cap):
            if spec.name==model.Coordinator.name:raise RuntimeError('injected coordinator runtime')
            return original(spec,cap)
        self.fail_closed(lambda:patch.object(gate.helper,'explore',broken),'injected coordinator runtime')
    def test_required_completion_witness_cannot_be_omitted(self):
        spec=model.Coordinator();report,graph=gate.helper.explore(spec,100000);states,edges,parents=graph
        damaged=[[e for e in row if e[0]!='ChooseCoordinator-ambiguous_absence_new_actual_acquire'] for row in edges]
        original=gate.helper.explore
        def omitted(candidate,cap):
            if candidate.name==spec.name and candidate.fault is None:return report,(states,damaged,parents)
            return original(candidate,cap)
        self.fail_closed(lambda:patch.object(gate.helper,'explore',omitted),"missing completed witness: ['ChooseCoordinator-ambiguous_absence_new_actual_acquire']")
    def test_both_loaded_files_are_authenticated_and_fail_closed(self):
        read=Path.read_bytes
        for name in ('coordinator_models.py','coordinator_gate.py'):
            path=Path(__file__).with_name(name).resolve()
            self.assertEqual(gate.helper.LOADED_SOURCES[str(path)],hashlib.sha256(read(path)).hexdigest())
            self.fail_closed(lambda path=path:patch.dict(gate.helper.LOADED_SOURCES,{str(path):'0'*64}),'loaded executable source differs from provenance: '+str(path))
            for mode in ('missing','syntax','runtime'):
                def altered(p,name=name,mode=mode):
                    if p.name!=name:return read(p)
                    if mode=='missing':raise FileNotFoundError('injected coordinator missing')
                    return b'invalid ! syntax' if mode=='syntax' else b'raise RuntimeError("injected coordinator runtime")'
                self.fail_closed(lambda:patch.object(Path,'read_bytes',altered),'invalid syntax' if mode=='syntax' else 'injected coordinator '+mode,reload=True)
