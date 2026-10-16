"""Independent prefix, mutation and mandatory-integration controls for direct state."""
from dataclasses import replace
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import check_source_group as bootstrap

gate=bootstrap.load_runtime()
model=gate.state_read_model
oracle=gate.state_read_gate
class SourceStateReadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.report,cls.controls=oracle.run_campaign(gate.helper,model,100000)
    def path(self,name):
        spec=model.SourceStateRead()
        state=next(s for action,s in spec.transitions(spec.initial) if action=='ChooseState-'+name)
        yield state
        while state.phase!='done':
            choices=list(spec.transitions(state));self.assertEqual(len(choices),1)
            state=choices[0][1];yield state
    def test_all_literal_cases_costs_and_mutants_complete(self):
        self.assertTrue(self.report['complete'])
        self.assertEqual(set(self.report['completed_witnesses']),set(model.CASES))
        self.assertEqual({row['fault'] for row in self.controls},set(model.FAULTS))
        for row in self.controls:gate.helper.replay(model.SourceStateRead(row['fault']),row['trace'],row['property'])
        for name,c in model.CASES.items():
            path=list(self.path(name))
            self.assertTrue(oracle.completed(path[-1],c),name)
            self.assertTrue(all(oracle.prefix_violation(s,c) is None for s in path),name)
    def test_foreign_points_and_invalid_nonlive_metadata_refuse_before_fields_or_reads(self):
        names=('foreign_cut','foreign_job','foreign_absent_cut','foreign_absent_job',
               'foreign_tombstone_cut','foreign_tombstone_job','tombstone_zero_hwm',
               'tombstone_fields','absent_retained_hwm')
        for name in names:
            states=list(self.path(name));terminal=states[-1]
            self.assertEqual(terminal.result,'unknown',name)
            self.assertFalse(any(word in ('fields','read','retain','copy') for word,_ in terminal.events),name)
            self.assertEqual(terminal.work,4,name)
    def test_later_membership_corruption_retires_valid_prefix(self):
        for name in ('C2_bad_span','C2_bad_hwm','C2_bad_pin'):
            states=list(self.path(name))
            self.assertTrue(any(s.rows and s.physical==(2,) for s in states),name)
            terminal=states[-1]
            self.assertEqual(terminal.events[-1],('refuse',('membership','unknown')))
            self.assertFalse(terminal.rows or terminal.funded or terminal.physical or terminal.owner)
            self.assertEqual(terminal.work,2)
    def test_copy_and_refusal_mutants_use_ordinary_events_with_real_damage(self):
        controls={row['fault']:row for row in self.controls}
        for fault,word in (('ss_copy_unfunded','copy'),('ss_keep_unknown_prefix','refuse'),('ss_refund_failure','refuse')):
            state=controls[fault]['trace'][-1]['state']
            self.assertEqual(state['events'][-1][0],word)
            if fault=='ss_copy_unfunded':
                self.assertTrue(state['rows']);self.assertFalse(state['funded'])
            elif fault=='ss_keep_unknown_prefix':
                self.assertTrue(state['rows']);self.assertTrue(state['funded']);self.assertEqual(state['result'],'unknown')
    def test_forged_intermediate_ownership_and_work_fail_before_completion(self):
        states=list(self.path('grouped_reversed_aliases'))
        copied=next(s for s in states if s.rows and s.physical)
        self.assertEqual(oracle.prefix_violation(replace(copied,funded=()),model.BASE),'SV-OWNER')
        self.assertEqual(oracle.prefix_violation(replace(copied,physical=()),model.BASE),'SV-OWNER')
        self.assertEqual(oracle.prefix_violation(replace(copied,work=4),model.BASE),'SV-WORK')
        self.assertEqual(oracle.prefix_violation(replace(copied,rows=()),model.BASE),'SV-COPY')
        spec=model.SourceStateRead()
        self.assertEqual(spec.violation(copied,'EraseCase',spec.initial),'SV-HISTORY')
        self.assertEqual(spec.violation(copied,'SwapCase',replace(copied,case='exact_cap')),'SV-HISTORY')
    def test_late_unknown_cannot_hide_prefix_credit_or_content_damage(self):
        path=list(self.path('C2_read_failure'));c=model.CASES['C2_read_failure']
        provisional=next(s for s in path if s.rows)
        terminal=path[-1]
        self.assertEqual(terminal.result,'unknown');self.assertFalse(terminal.rows)
        self.assertEqual(oracle.prefix_violation(replace(terminal,rows=provisional.rows),c),'SV-COPY')
        self.assertEqual(oracle.prefix_violation(replace(terminal,funded=provisional.funded),c),'SV-OWNER')
        self.assertEqual(oracle.prefix_violation(replace(terminal,work=4),c),'SV-WORK')
    def test_borrowed_success_owns_credit_through_explicit_drop(self):
        states=list(self.path('grouped_reversed_aliases'))
        owned=next(s for s in states if s.phase=='drop')
        self.assertTrue(owned.owner);self.assertTrue(owned.funded);self.assertEqual(owned.output,model.BASE.expected)
        self.assertEqual(oracle.prefix_violation(replace(owned,owner=False),model.BASE),'SV-OWNER')
        self.assertFalse(states[-1].owner);self.assertFalse(states[-1].funded)
    def test_interval_order_and_manifest_validation_are_not_optional(self):
        f=model.FRAMES[0]
        for damaged in (replace(f,commands=tuple(reversed(f.commands))),
                        replace(f,commands=(replace(f.commands[0],start=10),)+f.commands[1:])):
            cases=dict(model.CASES);cases['grouped_reversed_aliases']=replace(model.BASE,frames=(damaged,model.FRAMES[1]))
            with patch.object(model,'CASES',cases):
                with self.assertRaises(AssertionError):oracle.validate_fixtures(model)
        with patch.dict(oracle.READS,{'grouped_reversed_aliases':(1,)}):
            self.assertEqual(oracle.prefix_violation(list(self.path('grouped_reversed_aliases'))[-1],model.BASE),'SV-HISTORY')
    def test_missing_campaign_witness_or_control_blocks_aggregate_gate(self):
        variants=[({**self.report,'name':'other'},self.controls),
                  ({**self.report,'complete':False},self.controls),
                  ({**self.report,'completed_witnesses':{}},self.controls),
                  (self.report,[])]
        for result in variants:
            with patch.object(oracle,'run_campaign',lambda *args,result=result:result):
                with self.assertRaises(AssertionError):gate.run()
    def test_new_model_and_checker_bytes_are_captured(self):
        for module in (model,oracle):
            name=str(Path(module.__file__).resolve());self.assertIn(name,gate.helper.LOADED_SOURCES)
            with patch.dict(gate.helper.LOADED_SOURCES,{name:'0'*64}):
                with self.assertRaises(ValueError):gate.helper.validate_loaded_sources()
    def test_damaged_replayed_prefix_and_truncated_search_fail(self):
        with self.assertRaises(gate.helper.SearchLimit):oracle.run_campaign(gate.helper,model,1)
        row=self.controls[0];trace=json.loads(json.dumps(row['trace']))
        trace[-1]['state']['work']=100
        with self.assertRaises(ValueError):gate.helper.replay(model.SourceStateRead(row['fault']),trace,row['property'])
    def test_unavailable_new_campaign_replaces_prior_pass_with_failure(self):
        root=Path(__file__).resolve().parents[2];output=root/'target/api007-formal/test-source-state-read-unavailable.json'
        read=Path.read_bytes
        try:
            output.parent.mkdir(parents=True,exist_ok=True)
            for missing in ('source_state_read_models.py','source_state_read_gate.py'):
                output.write_text(json.dumps({'status':'PASS'}))
                def unavailable(path):
                    if path.name==missing:raise FileNotFoundError('injected missing '+missing)
                    return read(path)
                with patch.object(Path,'read_bytes',unavailable):
                    self.assertEqual(bootstrap.main(['--output',str(output)]),1)
                self.assertEqual(json.loads(output.read_text())['status'],'FAIL')
        finally:output.unlink(missing_ok=True)
if __name__=='__main__':unittest.main()
