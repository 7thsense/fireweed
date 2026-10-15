"""Independent prefix, owner, replay and sixth-campaign fail-closed controls."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import check_source_group as bootstrap

gate=bootstrap.load_runtime()
model=gate.original_model
oracle=gate.original_gate

class SourceOriginalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.report=gate.run()
    def terminal(self,name):
        spec=model.SourceOriginalResolution()
        s=next(s for _,s in spec.transitions(spec.initial) if s.case==name)
        while s.phase!='done':
            before=s;s=list(spec.transitions(s))[0][1]
            self.assertIsNone(spec.violation(before,'UnrelatedLabel',s),name)
        return spec,s
    def test_all_six_identities_witnesses_and_disjoint_replayed_controls(self):
        self.assertEqual([r['name'] for r in self.report['baseline'][:6]],
            ['api007-source-group-preimage','api007-source-group-encoding-preparation',
             'api007-source-record-read','api007-coupled-genesis-provision',
             'api007-source-header-recovery','api007-source-original-resolution'])
        r=self.report['baseline'][5]
        self.assertTrue(r['complete'])
        self.assertEqual(set(r['completed_witnesses']),set(model.ORIGINAL_CASES))
        controls=[c for c in self.report['negative_controls'] if c['fault'] in model.ORIGINAL_FAULTS]
        self.assertEqual(len(controls),len(model.ORIGINAL_FAULTS))
        self.assertEqual(len({c['fault'] for c in self.report['negative_controls']}),len(self.report['negative_controls']))
        for c in controls: gate.helper.replay(model.SourceOriginalResolution(c['fault']),c['trace'],c['property'])
    def test_independent_exact_witnesses_reject_deleted_events_and_forged_fields(self):
        for name,c in model.ORIGINAL_CASES.items():
            spec,s=self.terminal(name)
            self.assertTrue(oracle.completed_original_witness(s,c),name)
            for i in range(len(s.events)):
                self.assertFalse(oracle.completed_original_witness(replace(s,events=s.events[:i]+s.events[i+1:]),c),(name,i))
            for fields in ({'active':'foreign'},{'ledger':'replacement'},{'point_cut':'forged'},
                {'point_key':('prefix',)},{'record_cut':'foreign'},{'external_capsule':False},
                {'identity_credit':True},{'record_live':True},{'capsule_borrow':True},
                {'semantic':s.semantic+1},{'records':s.records+1},{'borrowed':not s.borrowed}):
                self.assertFalse(oracle.completed_original_witness(replace(s,**fields),c),(name,fields))
    def test_exposed_authority_and_owner_prefixes_are_checked_before_unknown(self):
        spec=model.SourceOriginalResolution()
        s=next(s for _,s in spec.transitions(spec.initial) if s.case=='commit')
        for fields,prop in (({'case':''},'OR-PREFIX'),({'events':('forged',)},'OR-PREFIX'),
            ({'semantic':s.semantic+1},'OR-WORK'),({'records':s.records+1},'OR-WORK')):
            self.assertEqual(spec.violation(s,'Renamed',replace(s,**fields)),prop)
        while s.phase!='pin': s=list(spec.transitions(s))[0][1]
        returned=('present','A',2,model.ORIGINAL_CASES['commit'].literal,'input-ledger')
        for fields in ({'result':'present'},{'returned':returned},{'output':returned}):
            self.assertIsNotNone(spec.violation(s,'ForgeEarlyProof',replace(s,**fields)))
        while s.phase!='borrow': s=list(spec.transitions(s))[0][1]
        for fields in ({'record_live':False},{'identity_credit':False},{'capsule_borrow':False},
            {'output':()},{'output':('foreign',)},{'returned':('foreign',),'output':('foreign',)}):
            self.assertIsNotNone(spec.violation(s,'LaterUnknown',replace(s,**fields)))
        self.assertEqual(spec.violation(s,'ReleaseExternalCapsule',replace(s,external_capsule=False)),'OR-CAPSULE')
        for name,phase,event,prop in [('wrong_publication_pin','pin','pin','OR-PIN'),
            ('wrong_claim_pin','pin','pin','OR-PIN'),('wrong_receipt_identity','member','member','OR-MEMBER'),
            ('other_equal_outcome','span','span','OR-SPAN'),('invalid_claim_binding','binding','binding','OR-CLAIM')]:
            p=next(s for _,s in spec.transitions(spec.initial) if s.case==name)
            while p.phase!=phase: p=list(spec.transitions(p))[0][1]
            forged=replace(p,events=p.events+(event,),phase='done',result='unknown',identity_credit=False,record_live=False,capsule_borrow=False)
            self.assertEqual(spec.violation(p,'LaterUnknown',forged),prop,name)
    def test_success_cannot_retire_owners_without_explicit_drop(self):
        spec=model.SourceOriginalResolution()
        for name in ('commit','absence'):
            s=next(s for _,s in spec.transitions(spec.initial) if s.case==name)
            while s.phase!='drop': s=list(spec.transitions(s))[0][1]
            forged=replace(s,phase='done',output=(),identity_credit=False,record_live=False,capsule_borrow=False)
            self.assertEqual(spec.violation(s,'PretendCompletion',forged),'OR-OWNER')
            self.assertFalse(oracle.completed_original_witness(forged,model.ORIGINAL_CASES[name]))
            substituted=replace(forged,events=forged.events+('return-unknown',))
            self.assertEqual(spec.violation(s,'SubstituteErrorForDrop',substituted),'OR-PREFIX')
    def test_conflict_requires_its_own_authenticated_return_event(self):
        spec=model.SourceOriginalResolution()
        s=next(s for _,s in spec.transitions(spec.initial) if s.case=='conflict_no_record_capacity')
        forged=replace(s,phase='done',events=('return-unknown',),result='conflict',capsule_borrow=False)
        self.assertEqual(spec.violation(s,'ConflictWithoutAuthority',forged),'OR-PREFIX')
        self.assertEqual(spec.violation(s,'NonterminalConflictWithoutAuthority',replace(s,result='conflict')),'OR-PREFIX')
        self.assertFalse(oracle.completed_original_witness(forged,model.ORIGINAL_CASES[s.case]))
    def test_port_errors_cannot_become_evidence_even_before_later_unknown(self):
        spec=model.SourceOriginalResolution()
        for name,phase,event,fields,prop in (
            ('missing_record','record','record-read',{'record_live':True},'OR-RECORD'),
            ('point_error','point','point',{'point_cut':'A','point_key':(2,model.ORIGINAL_CASES['point_error'].literal)},'OR-SOURCE')):
            s=next(s for _,s in spec.transitions(spec.initial) if s.case==name)
            while s.phase!=phase:s=list(spec.transitions(s))[0][1]
            forged=replace(s,events=s.events+(event,),**fields)
            self.assertEqual(spec.violation(s,'InventPortSuccess',forged),prop)
            late=replace(forged,phase='done',result='unknown',identity_credit=False,record_live=False,capsule_borrow=False)
            self.assertEqual(spec.violation(s,'LaterUnknown',late),prop)
        _,terminal=self.terminal('missing_record')
        continued=replace(terminal,events=terminal.events+('pin',))
        self.assertEqual(spec.violation(terminal,'ContinueAfterError',continued),'OR-PREFIX')
    def test_sixth_exploration_runtime_error_replaces_seeded_success(self):
        original=gate.helper.explore
        def broken(spec,cap):
            if spec.name=='api007-source-original-resolution':raise RuntimeError('injected original exploration runtime')
            return original(spec,cap)
        self.fail_closed(lambda:patch.object(gate.helper,'explore',broken),'injected original exploration runtime')
    def test_only_sixth_model_search_limit_replaces_seeded_success(self):
        original=gate.helper.explore
        def limited(spec,cap):
            return original(spec,1 if spec.name=='api007-source-original-resolution' and spec.fault is None else cap)
        self.fail_closed(lambda: patch.object(gate.helper,'explore',limited),
            'api007-source-original-resolution: incomplete search at 1 states')
    def test_only_sixth_model_required_witness_cannot_be_removed(self):
        spec=model.SourceOriginalResolution();report,graph=gate.helper.explore(spec,100000)
        states,edges,parents=graph
        damaged=[[edge for edge in row if edge[0]!='SelectOriginal-absence'] for row in edges]
        original=gate.helper.explore
        def removed(candidate,cap):
            if candidate.name==spec.name and candidate.fault is None: return report,(states,damaged,parents)
            return original(candidate,cap)
        self.fail_closed(lambda:patch.object(gate.helper,'explore',removed),"missing completed witness: ['SelectOriginal-absence']")
    def fail_closed(self,context,marker):
        out=Path(__file__).resolve().parents[2]/'target/api007-formal/test-source-original-failure.json'
        try:
            out.parent.mkdir(parents=True,exist_ok=True);out.write_text('{"status":"PASS"}')
            with context(),patch.object(bootstrap,'load_runtime',lambda:gate):
                self.assertEqual(bootstrap.main(['--output',str(out)]),1)
            failure=json.loads(out.read_text());self.assertEqual(failure['status'],'FAIL');self.assertIn(marker,failure['error'])
        finally: out.unlink(missing_ok=True)
    def test_each_new_executed_file_is_captured_and_cannot_change(self):
        for name in ('source_original_models.py','source_original_gate.py'):
            path=Path(__file__).with_name(name).resolve()
            self.assertEqual(gate.helper.LOADED_SOURCES[str(path)],hashlib.sha256(path.read_bytes()).hexdigest())
            self.fail_closed(lambda path=path:patch.dict(gate.helper.LOADED_SOURCES,{str(path):'0'*64}),
                'loaded executable source differs from provenance: '+str(path))
    def test_missing_syntax_and_runtime_failure_of_new_modules_invalidate_pass(self):
        out=Path(__file__).resolve().parents[2]/'target/api007-formal/test-source-original-module-failure.json'
        read=Path.read_bytes
        try:
            for name in ('source_original_models.py','source_original_gate.py'):
                for mode in ('missing','syntax','runtime'):
                    marker='injected original '+mode
                    def altered(path,name=name,mode=mode,marker=marker):
                        if path.name!=name:return read(path)
                        if mode=='missing':raise FileNotFoundError(marker)
                        return b'invalid ! syntax' if mode=='syntax' else ('raise RuntimeError('+repr(marker)+')').encode()
                    out.parent.mkdir(parents=True,exist_ok=True);out.write_text('{"status":"PASS"}')
                    with patch.object(Path,'read_bytes',altered): self.assertEqual(bootstrap.main(['--output',str(out)]),1)
                    failure=json.loads(out.read_text());self.assertEqual(failure['status'],'FAIL')
                    self.assertIn('invalid syntax' if mode=='syntax' else marker,failure['error'])
        finally:out.unlink(missing_ok=True)
