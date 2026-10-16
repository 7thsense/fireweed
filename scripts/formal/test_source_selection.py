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
    def test_unsupported_and_nonexistent_keys_never_produce_authority(self):
        for name in ('unsupported_decisive','unsupported_keys'):
            terminal=list(self.path(name))[-1]
            self.assertFalse(any(word in ('admit','open','copy','seal','return') for word,arg in terminal.events))
        control=next(row for row in self.controls if row['fault']=='sel_raw_nonexistent_keys')
        self.assertEqual(control['trace'][-1]['state']['events'][-1],('return',(('work','missing'),)))
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
