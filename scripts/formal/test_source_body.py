"""Independent content-prefix, declaration, lifetime and work controls."""
from dataclasses import replace
import unittest
import hashlib
import json
from pathlib import Path
from unittest.mock import patch
import check_source_group as bootstrap
gate=bootstrap.load_runtime()
model=gate.body_model
oracle=gate.body_gate
helper=gate.helper

class SourceBodyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.report,cls.controls=oracle.run_campaign(helper,model,100000)
    def path(self,name):
        spec=model.SourceBodies();s=next(s for _,s in spec.transitions(spec.initial) if s.case==name);path=[s]
        while s.phase!='done':
            before=s;s=list(spec.transitions(s))[0][1]
            self.assertIsNone(spec.violation(before,'renamed',s),name);path.append(s)
        return spec,path
    def test_all_exact_manifest_witnesses_and_replayed_mutants(self):
        self.assertEqual(set(self.report['completed_witnesses']),set(model.CASES))
        self.assertEqual({r['fault'] for r in self.controls},set(model.FAULTS))
        for row in self.controls:helper.replay(model.SourceBodies(row['fault']),row['trace'],row['property'])
    def test_every_prefix_checks_content_loaded_credit_work_and_declarations(self):
        for name in model.CASES:
            spec,path=self.path(name)
            for s in path:
                for fields in ({'image':(('foreign',(99,)),)}, {'loaded':('foreign',)},
                    {'views':((99,'foreign',(99,)),)},{'copies':s.copies+1},
                    {'work':(s.work[0]+1,*s.work[1:])},{'ledger':'new'},
                    {'physical':s.physical+(99,)},{'hashes':s.hashes+('foreign',)},
                    {'observed':s.observed+(('foreign','absent',0,0,0),)},
                    {'declarations':s.declarations+(model.Declaration('foreign'),)}):
                    self.assertIsNotNone(spec.violation(s,'renamed',replace(s,**fields)),(name,fields))
    def test_deleted_events_and_early_terminal_never_complete(self):
        for name,c in model.CASES.items():
            spec,path=self.path(name);final=path[-1]
            self.assertTrue(oracle.completed_witness(final,c))
            for i in range(len(final.events)):
                self.assertFalse(oracle.completed_witness(replace(final,events=final.events[:i]+final.events[i+1:]),c),(name,i))
            for s in path[:-1]:self.assertIsNotNone(spec.violation(s,'forged',replace(s,phase='done')))
    def test_manifest_is_independent_and_requires_exact_result_costs_and_returns(self):
        for name,c in model.CASES.items():
            _,path=self.path(name);s=path[-1];e=c.expected
            for bad in (replace(e,result='image' if e.result=='unknown' else 'unknown'),replace(e,work=(99,*e.work[1:])),replace(e,hashes=e.hashes+('foreign',)),replace(e,returned=(('foreign',99,99,()),))):
                self.assertFalse(oracle.completed_witness(s,replace(c,expected=bad)),name)
    def test_requested_tombstone_absence_and_missing_observation_are_distinct(self):
        for name,status,hwm in [('authenticated_absence','absent',0),('authenticated_tombstone','tombstone',4)]:
            _,path=self.path(name)
            observed=next(s for s in path if s.events and s.events[-1][0]=='observations')
            self.assertEqual(observed.observed[-1],('c',status,0,0,hwm))
            self.assertEqual(len(observed.declarations),4)
            self.assertEqual(path[-1].reads,(1,))
        _,missing=self.path('omitted_observation')
        self.assertEqual(missing[-1].result,'unknown');self.assertEqual(missing[-1].reads,())
    def test_metadata_only_never_reads_unreadable_old_body(self):
        _,path=self.path('metadata_only_unreadable_body')
        returned=next(s for s in path if s.events and s.events[-1][0]=='return')
        self.assertEqual(returned.reads,());self.assertEqual(returned.loaded,())
        self.assertTrue(all(row[3] is None for row in returned.returned))
        _,required=self.path('unreadable_required_body')
        self.assertEqual((required[-1].result,required[-1].reads),('unknown',(1,)))
    def test_real_state_and_receipt_intervals_keep_work_fact_and_key(self):
        for name,domain in [('state_interval_substitution','state'),('receipt_interval_substitution','receipt')]:
            c=model.CASES[name];fact=c.observations[1]
            actual=next(x for x in c.frames[0].commands+c.frames[0].regions if x.domain==domain)
            self.assertEqual(fact.key,'a');self.assertEqual(fact.status,'live')
            self.assertEqual((fact.evidence.start,fact.evidence.digest),(actual.start,actual.body))
            _,path=self.path(name)
            self.assertEqual((path[-1].result,path[-1].hashes),('unknown',('a',)))
    def test_bad_later_alias_and_command_keep_already_spent_hashes_without_copies(self):
        for name,hashes in [('alias_metadata',('a',)),('later_command_bad_interval',('a','b'))]:
            _,path=self.path(name);last=path[-1]
            self.assertEqual(last.hashes,hashes);self.assertEqual(last.result,'unknown')
            self.assertFalse(any(w=='copy' for w,_ in last.events))
        _,path=self.path('wrong_frame_missing_member')
        self.assertEqual(path[-1].hashes,('a','b','c'))
        self.assertTrue(any(w=='copy' for w,_ in path[-1].events))
        self.assertEqual(path[-1].image,())
    def test_hash_order_comes_from_actual_frame_not_request_order(self):
        _,path=self.path('actual_frame_order')
        self.assertEqual(path[-1].hashes,('b','a','c'))
        self.assertEqual([d.key for d in model.CASES['actual_frame_order'].declarations],['c','a','a','b'])
    def test_shared_alias_funding_and_retirement_before_next_dispatch(self):
        _,path=self.path('shared_aliases')
        second=next(s for s in path if s.events and s.events[-1]==('attempt',2))
        self.assertEqual((second.image,second.copies,second.physical),((('a',(1,2)),('b',(1,2))),4,()))
        self.assertEqual((path[-1].reads,path[-1].walks,path[-1].hashes),((1,2),(1,2),('a','b','c')))
    def test_cap_hydration_failure_copy_failure_and_cancel_retry_work(self):
        for name,work in [('second_cap',(17,91,2,18,195)),('second_hydration_failure',(17,91,2,15,186)),('second_copy_refusal',(17,91,2,15,186))]:
            _,path=self.path(name);self.assertEqual(path[-1].work,work)
            self.assertEqual((path[-1].image,path[-1].copies,path[-1].physical),((),0,()))
        _,path=self.path('second_cancel_retry')
        cancelled=next(s for s in path if s.events and s.events[-1][0]=='cancel')
        retired=next(s for s in path if s.events and s.events[-1][0]=='retire')
        self.assertEqual((cancelled.image,cancelled.copies,cancelled.physical,cancelled.pending,cancelled.owner),((),0,(2,),True,False))
        self.assertEqual(retired.work,cancelled.work)
        self.assertEqual((cancelled.observed,cancelled.declarations,cancelled.frames),((),(),()))
        self.assertEqual(tuple(a-b for a,b in zip(model.INITIAL_WORK,cancelled.work)),tuple(a-b for a,b in zip(cancelled.work,path[-1].work)))
    def test_mixed_body_scope_loads_only_required_readable_frame(self):
        spec,path=self.path('mixed_body_scope');last=path[-1]
        self.assertEqual((last.reads,last.walks,last.hashes),((2,),(2,),('c',)))
        returned=next(s for s in path if s.events and s.events[-1][0]=='return')
        self.assertEqual(returned.returned,(('a',1,1,None),('b',1,2,None),('c',1,3,(7,8,9))))
        observed=next(s for s in path if s.events and s.events[-1][0]=='observations')
        self.assertEqual(spec.violation(observed,'broadening',replace(observed,frames=(1,2))),'B-COUNT')
        row=next(r for r in self.controls if r['fault']=='body_broaden_body_scope')
        self.assertEqual(row['trace'][-1]['state']['case'],'mixed_body_scope')
    def test_all_frame_fixtures_have_canonical_order_and_nonoverlapping_intervals(self):
        for name,c in model.CASES.items():
            for frame in c.frames:
                identities=[(0 if x.domain=='work' else 1,x.key) for x in frame.commands]
                self.assertEqual(identities,sorted(set(identities)),(name,frame.offset))
                starts=[x.start for x in frame.commands]
                self.assertEqual(starts,sorted(starts),(name,frame.offset))
                intervals=sorted((x.start,x.start+len(x.body)) for x in frame.commands+frame.regions)
                self.assertTrue(all(a[1]<=b[0] for a,b in zip(intervals,intervals[1:])),(name,frame.offset))
                self.assertTrue(all(x.domain!='receipt' for x in frame.commands))
    def test_version_correspondence_fixture_has_valid_current_metadata(self):
        c=model.CASES['future_command_version']
        self.assertEqual((c.observations[0].version,c.observations[0].highwater),(1,1))
        self.assertEqual(c.frames[0].commands[0].version,2)
        _,path=self.path('future_command_version')
        self.assertEqual((path[-1].result,path[-1].hashes),('unknown',('a',)))
    def test_return_borrow_drop_retains_data_funding_and_owner(self):
        _,path=self.path('shared_aliases')
        returned=next(s for s in path if s.events and s.events[-1][0]=='return')
        borrowed=next(s for s in path if s.events and s.events[-1][0]=='borrow')
        self.assertEqual((returned.image,returned.copies,returned.owner),(borrowed.image,borrowed.copies,borrowed.owner))
        self.assertEqual((returned.copies,returned.owner),(7,True))
        self.assertEqual((path[-1].image,path[-1].copies,path[-1].owner),((),0,False))
    def fail_closed(self,context,marker,reload=False):
        out=Path(__file__).resolve().parents[2]/'target/api007-formal/test-body-failure.json'
        out.parent.mkdir(parents=True,exist_ok=True);out.write_text('{"status":"PASS"}')
        try:
            with context(),patch.object(bootstrap,'load_runtime',bootstrap.load_runtime if reload else lambda:gate):
                self.assertEqual(bootstrap.main(['--output',str(out)]),1)
            r=json.loads(out.read_text());self.assertEqual(r['status'],'FAIL');self.assertIn(marker,r['error'])
        finally:out.unlink(missing_ok=True)
    def test_tenth_identity_and_complete_bound_are_mandatory(self):
        self.fail_closed(lambda:patch.object(model.SourceBodies,'name','omitted-body'),'tenth exact source body identity required')
        original=helper.explore
        def limited(spec,cap):return original(spec,1 if spec.name==model.SourceBodies.name and spec.fault is None else cap)
        self.fail_closed(lambda:patch.object(helper,'explore',limited),'api007-addressed-body-materialization: incomplete search at 1 states')
    def test_missing_witness_mutant_and_nonpassing_campaign_fail_closed(self):
        original=oracle.run_campaign
        for mode,message in [('witness','all body completion witnesses required'),('mutant','all body mutant controls required'),('status','complete passing body campaign required')]:
            def damaged(h,m,cap,mode=mode):
                r,c=original(h,m,cap)
                if mode=='witness':r['completed_witnesses'].pop('metadata_only_unreadable_body')
                elif mode=='mutant':c=c[:-1]
                else:r['status']='INCOMPLETE'
                return r,c
            self.fail_closed(lambda:patch.object(oracle,'run_campaign',damaged),message)
    def test_both_executed_files_are_pinned_and_load_errors_invalidate_old_pass(self):
        read=Path.read_bytes
        for name in ('source_body_models.py','source_body_gate.py'):
            path=Path(__file__).with_name(name).resolve()
            self.assertEqual(helper.LOADED_SOURCES[str(path)],hashlib.sha256(read(path)).hexdigest())
            self.fail_closed(lambda path=path:patch.dict(helper.LOADED_SOURCES,{str(path):'0'*64}),'loaded executable source differs from provenance: '+str(path))
            for mode in ('missing','syntax','runtime'):
                def altered(p,name=name,mode=mode):
                    if p.name!=name:return read(p)
                    if mode=='missing':raise FileNotFoundError('injected body missing')
                    return b'invalid ! syntax' if mode=='syntax' else b'raise RuntimeError("injected body runtime")'
                self.fail_closed(lambda:patch.object(Path,'read_bytes',altered),'invalid syntax' if mode=='syntax' else 'injected body '+mode,reload=True)
    def test_incomplete_bound_never_passes(self):
        with self.assertRaises(helper.SearchLimit):oracle.run_campaign(helper,model,1)

if __name__=='__main__':unittest.main()
