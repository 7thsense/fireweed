"""Independent selection-prefix, corruption and mandatory-campaign tests."""
from dataclasses import replace
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import check_source_group as bootstrap

gate=bootstrap.load_runtime()
model=gate.selection_model
oracle=gate.selection_gate
class SourceSelectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.report,cls.controls=oracle.run_campaign(gate.helper,model,100000)
    def path(self,name):
        spec=model.SourceSelection()
        state=next(s for action,s in spec.transitions(spec.initial) if action=='ChooseSelection-'+name)
        yield state
        while not state.done:
            choices=list(spec.transitions(state));self.assertEqual(len(choices),1)
            state=choices[0][1];yield state
    def test_literal_membership_costs_and_replayed_mutants(self):
        self.assertTrue(self.report['complete'])
        self.assertEqual(set(self.report['completed_witnesses']),set(model.CASES))
        self.assertEqual({row['fault'] for row in self.controls},set(model.FAULTS))
        for row in self.controls:gate.helper.replay(model.SourceSelection(row['fault']),row['trace'],row['property'])
        for name,c in model.CASES.items():
            states=list(self.path(name))
            self.assertTrue(oracle.completed(states[-1],c),name)
            self.assertTrue(all(oracle.prefix_violation(s,c) is None for s in states),name)
    def test_immutable_original_interpreter_is_independent_of_compiler_and_postings(self):
        c=model.CASES['nested']
        with patch.object(model,'compile_program',side_effect=RuntimeError('compiler unavailable')):
            with patch.object(model,'stream',side_effect=RuntimeError('posting stream unavailable')):
                self.assertEqual(oracle.reference(c),(model.C,))
                manifest,algebra,count,timeline=oracle.original_manifest(c)
                self.assertEqual(len(manifest),3)
                self.assertEqual(algebra[-1][-1],(model.C,))
        self.assertEqual(oracle.reference(model.CASES['all_empty']),(model.A,model.B,model.C))
        self.assertEqual(oracle.reference(model.CASES['other_collection']),(model.O,))
    def test_all_types_and_actual_empty_ranges_are_required(self):
        for kind in model.KINDS:
            states=list(self.path('presence_'+kind))
            terminal=states[-1]
            self.assertEqual([arg[2] for word,arg in terminal.events if word=='finish'],list(model.KINDS))
            self.assertEqual([arg for word,arg in terminal.events if word=='return'],[(model.A,)])
        terminal=list(self.path('empty_source'))[-1]
        self.assertEqual(sum(word=='terminal_empty' for word,arg in terminal.events),2)
        self.assertFalse(any(word=='copy' for word,arg in terminal.events))
    def test_foreign_cut_and_producer_and_occurrences_are_not_numeric_head_checks(self):
        controls={row['fault']:row for row in self.controls}
        for fault in ('sel_foreign_cut','sel_foreign_producer'):
            state=controls[fault]['trace'][-1]['state']
            self.assertEqual(state['events'][0][1][5],7)
            self.assertEqual(state['events'][-1][0],'open')
        state=controls['sel_exchange_occurrence']['trace'][-1]['state']
        self.assertEqual(state['events'][-1][0],'seal')
    def test_ports_retire_before_actual_coordinator_capsule(self):
        for name in ('direct_cancel','accepted_caller_abort','late_algebra_cancel'):
            states=list(self.path(name));retired=states[-2]
            self.assertFalse(retired.owner)
            self.assertTrue(retired.capsule)
            self.assertFalse(retired.sets or retired.copies or retired.pending)
            self.assertFalse(states[-1].capsule)
            self.assertEqual(states[-1].work,retired.work)
        states=list(self.path('accepted_caller_abort'))
        abandoned=next(s for s in states if s.events and s.events[-1][0]=='caller_abort')
        self.assertTrue(abandoned.owner and abandoned.capsule and abandoned.pending)
        self.assertEqual(states[-1].result,'ok')
    def test_late_unknown_cannot_hide_refunds_prefix_or_owner_damage(self):
        for name in ('late_write_failure','direct_cancel','late_algebra_failure','late_algebra_cancel'):
            states=list(self.path(name));terminal=states[-1];c=model.CASES[name]
            self.assertEqual(terminal.result,'unknown')
            self.assertEqual(oracle.prefix_violation(replace(terminal,output=(model.A,)),c),'SEL-RETURN')
            self.assertEqual(oracle.prefix_violation(replace(terminal,work=(100,100,100)),c),'SEL-WORK')
            alive=next(s for s in states if s.pending)
            self.assertEqual(oracle.prefix_violation(replace(alive,capsule=False),c),'SEL-OWNER')
    def test_forged_set_and_algebra_damage_cannot_be_hidden_by_consistent_flags(self):
        states=list(self.path('nested'));c=model.CASES['nested']
        sealed=next(s for s in states if s.sets)
        self.assertEqual(oracle.prefix_violation(replace(sealed,sets=()),c),'SEL-COMPLETE')
        index=next(i for i,s in enumerate(states) if s.events and s.events[-1][0]=='algebra')
        state=states[index];op,left,right,value=state.events[-1][1]
        forged=replace(state,events=state.events[:-1]+(('algebra',(op,(),right,value)),))
        self.assertEqual(oracle.prefix_violation(forged,c),'SEL-ALGEBRA')
        pending=next(s for s in states if s.pending)
        self.assertEqual(oracle.prefix_violation(replace(pending,copies=()),c),'SEL-WRITE')
    def test_unsupported_nodes_and_raw_key_bypass_never_produce_membership(self):
        for name in ('unsupported_decisive','unsupported_intrinsic'):
            terminal=list(self.path(name))[-1]
            self.assertFalse(any(word in ('admit','open','copy','seal','return') for word,arg in terminal.events))
        control=next(row for row in self.controls if row['fault']=='sel_raw_nonexistent_keys')
        state=control['trace'][-1]['state'];word,key=state['events'][-1]
        self.assertEqual(word,'key_write_pending')
        c=model.CASES[state['case']]
        self.assertNotIn(key,{record[0] for record in c.records})
        self.assertIn(key,c.expression[1])
    def test_forged_initial_selection_and_interleaved_algebra_prefixes(self):
        spec=model.SourceSelection()
        for forged in (replace(spec.initial,output=(model.A,),owner=True,done=True),
                       replace(spec.initial,case='equal',capsule=True),
                       replace(spec.initial,case='equal',work=(0,0,0))):
            self.assertEqual(spec.violation(spec.initial,'Forged',forged),'SEL-SCOPE')
        for name in ('early_algebra_failure','early_algebra_cancel','algebra_then_source_failure'):
            states=list(self.path(name));terminal=states[-1]
            events=[word for word,arg in terminal.events]
            self.assertIn('algebra',events)
            self.assertFalse(any(word=='return' for word,arg in terminal.events))
            first=events.index('algebra')
            self.assertEqual(sum(word=='seal' for word in events[:first]),2)
            if name=='algebra_then_source_failure':
                self.assertIn('manifest',events[first+1:])
                self.assertIn('write_pending',events[first+1:])
    def test_exact_and_one_short_work_and_root_grants(self):
        for name in ('work_exact','algebra_work_exact','root_exact'):
            terminal=list(self.path(name))[-1]
            self.assertEqual(terminal.result,'ok')
        for name in ('work_row_short','work_copy_short','work_step_short','algebra_work_short',
                     'root_copy_short','root_manifest_short'):
            states=list(self.path(name));c=model.CASES[name]
            refusal=next(s for s in states if s.events and s.events[-1][0]=='unknown')
            previous=states[states.index(refusal)-1]
            self.assertEqual(refusal.work,previous.work)
            self.assertEqual((refusal.root,refusal.funded),(previous.root,previous.funded))
            self.assertEqual(states[-1].root,c.root)
            self.assertEqual(states[-1].funded,0)
            self.assertFalse(any(word=='return' for word,arg in states[-1].events))
        terminal=list(self.path('root_manifest_short'))[-1]
        self.assertFalse(any(word in ('admit','open','copy') for word,arg in terminal.events))
        terminal=list(self.path('root_copy_short'))[-1]
        self.assertFalse(any(word=='copy' for word,arg in terminal.events))
    def test_scoped_cursor_certificates_and_empty_builders(self):
        terminal=list(self.path('multi_range'))[-1]
        certificates=[arg for word,arg in terminal.events if word=='finish']
        self.assertEqual([certificate[2] for certificate in certificates],[('integer',1),('integer',2)])
        self.assertNotEqual(certificates[0][3],certificates[1][3])
        for name in ('any_empty','identical_empty_clause0','identical_empty_clause1'):
            terminal=list(self.path(name))[-1]
            words=[word for word,arg in terminal.events]
            self.assertEqual(words.count('empty_builder'),1)
            self.assertEqual(words.count('empty_seal'),1)
            self.assertLess(words.index('empty_seal'),words.index('return'))
        contexts=[next(arg[0] for word,arg in list(self.path(name))[-1].events if word=='empty_seal')
                  for name in ('identical_empty_clause0','identical_empty_clause1')]
        self.assertEqual(contexts[0][:-1],contexts[1][:-1])
        self.assertNotEqual(contexts[0][-1],contexts[1][-1])
        controls={row['fault']:row for row in self.controls}
        for fault in ('sel_same_kind_certificate','sel_foreign_leaf_proof','sel_same_balance_foreign_budget','sel_wrong_empty_clause'):
            self.assertEqual(controls[fault]['property'],'SEL-SCOPE')
    def test_each_certificate_context_component_is_independently_bound(self):
        c=model.CASES['equal']
        state=next(s for s in self.path('equal') if s.events and s.events[-1][0]=='finish')
        scope,leaf,kind,cursor=state.events[-1][1]
        for index in range(len(scope)):
            replacement=scope[index]+1 if isinstance(scope[index],int) else 'different-'+scope[index]
            foreign=scope[:index]+(replacement,)+scope[index+1:]
            forged=replace(state,events=state.events[:-1]+(('finish',(foreign,leaf,kind,cursor)),))
            self.assertEqual(oracle.prefix_violation(forged,c),'SEL-SCOPE',index)
    def test_spent_algebra_grant_cannot_be_reclassified_as_denial(self):
        c=model.CASES['algebra_work_exact']
        granted=next(s for s in self.path('algebra_work_exact') if s.events and s.events[-1][0]=='algebra_grant')
        self.assertEqual(granted.work,(0,0,0))
        forged=replace(granted,events=granted.events+(('unknown',('budget',(1,0,1))),),result='unknown')
        self.assertEqual(oracle.prefix_violation(forged,c),'SEL-RETURN')
        self.assertEqual(model.SourceSelection().violation(granted,'ForgedDenial',forged),'SEL-RETURN')
    def test_three_child_fold_uses_actual_rightmost_stack_pop_inputs(self):
        terminal=list(self.path('three_distinct_fold_children'))[-1]
        operations=[arg for word,arg in terminal.events if word=='algebra']
        self.assertEqual(operations[-2:],[
            ('union',(model.C,),(model.B,),(model.B,model.C)),
            ('union',(model.B,model.C),(model.A,),(model.A,model.B,model.C)),
        ])
        self.assertEqual(oracle.reference(model.CASES['three_distinct_fold_children']),(model.A,model.B,model.C))
    def test_key_oracle_does_not_use_producer_point_or_compiler_helpers(self):
        c=model.CASES['keys_mixed']
        with patch.object(model,'point_fact',side_effect=RuntimeError('producer unavailable')):
            with patch.object(model,'valid_point',side_effect=RuntimeError('producer unavailable')):
                with patch.object(model,'compile_program',side_effect=RuntimeError('compiler unavailable')):
                    self.assertEqual(oracle.reference(c),(model.A,model.B,model.C))
                    self.assertEqual(oracle.source_key_fact(c,('work','missing'))[2],'absent')
                    self.assertEqual(oracle.original_manifest(c)[0][0][3],(model.A,model.B,model.C))
    def test_every_key_nonmember_is_observed_and_empty_keys_seal_without_ranges(self):
        terminal=list(self.path('keys_mixed'))[-1]
        observations=[arg[2] for word,arg in terminal.events if word=='key_complete']
        self.assertEqual(observations,[(model.A,'live',1),(model.B,'live',1),(model.C,'live',1),(('work','missing'),'absent',0)])
        proof=next(arg for word,arg in terminal.events if word=='key_seal')
        self.assertEqual(proof[2],tuple(observations))
        self.assertEqual(proof[3],(model.A,model.B,model.C))
        tomb=list(self.path('keys_tombstone'))[-1]
        self.assertIn((model.A,'tombstone',2),next(arg[2] for word,arg in tomb.events if word=='key_seal'))
        state_only=list(self.path('keys_state_only'))[-1]
        self.assertEqual(next(arg[3] for word,arg in state_only.events if word=='key_seal'),())
        for name in ('keys_empty','keys_empty_other_clause'):
            terminal=list(self.path(name))[-1]
            self.assertFalse(any(word in ('open','key_lookup','key_point') for word,arg in terminal.events))
            proof=next(arg for word,arg in terminal.events if word=='key_seal')
            self.assertEqual(proof[2:],((),()))
    def test_bad_points_and_decisive_branches_never_seal_a_partial_key_leaf(self):
        for name in ('keys_bad_tomb_hwm','keys_bad_tomb_fields','keys_foreign_absence','keys_late_point_failure','keys_late_cancel','keys_decisive_failure'):
            states=list(self.path(name));terminal=states[-1]
            self.assertTrue(any(word=='key_write_done' for word,arg in terminal.events),name)
            self.assertFalse(any(word in ('key_seal','return') for word,arg in terminal.events),name)
            self.assertEqual(terminal.result,'unknown')
            self.assertFalse(terminal.point or terminal.key_observations or terminal.builders or terminal.copies or terminal.pending or terminal.sets)
            self.assertEqual(terminal.root,model.CASES[name].root)
        terminal=list(self.path('keys_not'))[-1]
        self.assertEqual(next(arg for word,arg in terminal.events if word=='return'),(model.B,model.C))
    def test_key_work_denial_is_atomic_and_manifest_funding_precedes_dispatch(self):
        for name in ('keys_work_exact','keys_root_exact'):
            self.assertEqual(list(self.path(name))[-1].result,'ok')
        for name in ('keys_work_short','keys_root_short','keys_manifest_short'):
            states=list(self.path(name));refused=next(s for s in states if s.events and s.events[-1][0]=='unknown')
            before=states[states.index(refused)-1]
            self.assertEqual(refused.work,before.work)
            self.assertEqual((refused.root,refused.funded),(before.root,before.funded))
            self.assertEqual(states[-1].work,refused.work)
        terminal=list(self.path('keys_work_short'))[-1]
        self.assertEqual(terminal.work,(1,0,0))
        self.assertEqual(sum(word=='key_write_done' for word,arg in terminal.events),1)
        for name in ('keys_root_short','keys_manifest_short'):
            self.assertFalse(any(word=='key_lookup' for word,arg in list(self.path(name))[-1].events))
    def test_forged_key_certificates_cannot_erase_nonmembers_or_exchange_occurrences(self):
        state=next(s for s in self.path('keys_mixed') if s.events and s.events[-1][0]=='key_seal')
        scope,leaf,observations,members=state.events[-1][1]
        damaged=(scope,leaf,observations[:-1],members)
        forged=replace(state,events=state.events[:-1]+(('key_seal',damaged),),sets=state.sets[:-1]+(damaged,))
        self.assertEqual(oracle.prefix_violation(forged,model.CASES['keys_mixed']),'SEL-COMPLETE')
        states=[s for s in self.path('keys_identical') if s.events and s.events[-1][0]=='key_seal']
        first,second=states;foreign=first.events[-1][1]
        forged=replace(second,events=second.events[:-1]+(('key_seal',foreign),),sets=second.sets[:-1]+(foreign,))
        self.assertEqual(oracle.prefix_violation(forged,model.CASES['keys_identical']),'SEL-SCOPE')
        completed=next(s for s in self.path('keys_mixed') if s.events and s.events[-1][0]=='key_complete')
        context,leaf,observation=completed.events[-1][1]
        for index in range(len(context)):
            change=context[index]+1 if isinstance(context[index],int) else 'foreign-'+context[index]
            scope=context[:index]+(change,)+context[index+1:]
            forged=replace(completed,events=completed.events[:-1]+(('key_complete',(scope,leaf,observation)),))
            self.assertEqual(oracle.prefix_violation(forged,model.CASES['keys_mixed']),'SEL-SCOPE')
    def test_key_caller_abort_keeps_operation_and_late_unknown_cannot_hide_state(self):
        states=list(self.path('keys_caller_abort'))
        abandoned=next(s for s in states if s.events and s.events[-1][0]=='caller_abort')
        self.assertTrue(abandoned.owner and abandoned.capsule and abandoned.pending and abandoned.point)
        self.assertEqual(states[-1].result,'ok')
        for name in ('keys_late_point_failure','keys_late_cancel'):
            states=list(self.path(name));terminal=states[-1];c=model.CASES[name]
            reply=next(s for s in states if s.point)
            self.assertEqual(oracle.prefix_violation(replace(terminal,point=reply.point),c),'SEL-KEY-AUTH')
            self.assertEqual(oracle.prefix_violation(replace(terminal,work=c.capacity),c),'SEL-WORK')
            self.assertEqual(oracle.prefix_violation(replace(terminal,key_observations=((model.A,'live',1),)),c),'SEL-KEY-AUTH')
    def test_declared_point_error_cannot_turn_into_a_successful_reply_prefix(self):
        c=model.CASES['keys_late_point_failure']
        before=next(s for s in self.path('keys_late_point_failure') if s.events and s.events[-1][0]=='key_lookup' and s.events[-1][1][2]==model.B)
        reply=oracle.source_key_fact(c,model.B)
        self.assertTrue(oracle.key_fact_valid(c,model.B,reply))
        forged=replace(before,events=before.events+(('key_point',reply),),point=reply)
        self.assertEqual(oracle.prefix_violation(forged,c),'SEL-KEY-AUTH')
        self.assertEqual(model.SourceSelection().violation(before,'ForgedReply',forged),'SEL-KEY-AUTH')
        self.assertEqual(next(row['property'] for row in self.controls if row['fault']=='key_success_after_port_error'),'SEL-KEY-AUTH')
    def test_valid_metadata_shape_cannot_replace_immutable_live_reply_version(self):
        c=model.CASES['keys_mixed']
        original=next(s for s in self.path('keys_mixed') if s.events and s.events[-1][0]=='key_point')
        fact=original.point;forged_fact=(*fact[:3],2,*fact[4:6],2)
        self.assertTrue(oracle.key_fact_valid(c,model.A,forged_fact))
        forged=replace(original,events=original.events[:-1]+(('key_point',forged_fact),),point=forged_fact)
        self.assertEqual(oracle.prefix_violation(forged,c),'SEL-KEY-AUTH')
        self.assertEqual(next(row['property'] for row in self.controls if row['fault']=='key_forged_live_version'),'SEL-KEY-AUTH')
    def test_key_fixtures_are_admitted_canonical_work_requests(self):
        for expression in [('keys',(model.A,model.A)),('keys',(model.O,)),('keys',tuple(('work',str(n)) for n in range(5)))]:
            cases=dict(model.CASES);cases['keys_mixed']=replace(cases['keys_mixed'],expression=expression)
            with patch.object(model,'CASES',cases):
                with self.assertRaisesRegex(AssertionError,'canonical Work Keys'):oracle.run_campaign(gate.helper,model,100000)
    def test_missing_or_incomplete_campaign_blocks_aggregate(self):
        for result in [({**self.report,'name':'other'},self.controls),
                       ({**self.report,'complete':False},self.controls),
                       ({**self.report,'completed_witnesses':{}},self.controls),
                       (self.report,[])]:
            with patch.object(oracle,'run_campaign',lambda *args,result=result:result):
                with self.assertRaises(AssertionError):gate.run()
    def test_captured_bytes_and_bounded_exploration_fail_closed(self):
        for module in (model,oracle):
            name=str(Path(module.__file__).resolve());self.assertIn(name,gate.helper.LOADED_SOURCES)
            with patch.dict(gate.helper.LOADED_SOURCES,{name:'0'*64}):
                with self.assertRaises(ValueError):gate.helper.validate_loaded_sources()
        with self.assertRaises(gate.helper.SearchLimit):oracle.run_campaign(gate.helper,model,1)
        row=self.controls[0];trace=json.loads(json.dumps(row['trace']))
        trace[-1]['state']['work']=[100,100,100]
        with self.assertRaises(ValueError):gate.helper.replay(model.SourceSelection(row['fault']),trace,row['property'])
    def test_missing_files_replace_stale_pass(self):
        root=Path(__file__).resolve().parents[2];output=root/'target/api007-formal/test-source-selection-unavailable.json'
        read=Path.read_bytes
        try:
            output.parent.mkdir(parents=True,exist_ok=True)
            for missing in ('source_selection_models.py','source_selection_gate.py'):
                output.write_text(json.dumps({'status':'PASS'}))
                def unavailable(path):
                    if path.name==missing:raise FileNotFoundError('injected missing '+missing)
                    return read(path)
                with patch.object(Path,'read_bytes',unavailable):self.assertEqual(bootstrap.main(['--output',str(output)]),1)
                self.assertEqual(json.loads(output.read_text())['status'],'FAIL')
        finally:output.unlink(missing_ok=True)
if __name__=='__main__':unittest.main()
