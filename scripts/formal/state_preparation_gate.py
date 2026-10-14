#!/usr/bin/env python3
"""Bounded domain authority gate; mappings are documentary, unexecuted."""
import hashlib
from pathlib import Path
import subprocess
import sys
import types

helper_path = Path(__file__).resolve().with_name('check_api007.py')
helper_bytes = helper_path.read_bytes()
helper = types.ModuleType('lookup_gate_helpers')
helper.__file__ = str(helper_path)
sys.modules[helper.__name__] = helper
exec(compile(helper_bytes, str(helper_path), 'exec'), helper.__dict__)
helper.LOADED_SOURCES[str(helper_path)] = hashlib.sha256(helper_bytes).hexdigest()
model_path = helper_path.with_name('state_preparation_models.py')
model_bytes = model_path.read_bytes()
model = types.ModuleType('state_preparation_models')
model.__file__ = str(model_path)
sys.modules[model.__name__] = model
exec(compile(model_bytes, str(model_path), 'exec'), model.__dict__)
helper.LOADED_SOURCES[str(model_path)] = hashlib.sha256(model_bytes).hexdigest()

def run(max_states=100000):
    specification=model.StatePreparation()
    report,graph=helper.explore(specification,max_states)
    if report['status']!='PASS' or not report['complete']: raise AssertionError('incomplete state baseline')
    states,edges,_=graph
    report['graph_sha256']=helper.digest({'states':[helper.render(s) for s in states],'edges':edges})
    report['completed_witnesses']={}
    cases={'empty':(0,0,0,0,0,0,0),'foreign_empty':(0,0,0,1,0,0,0),
        'original_with_corrupt_cache':(0,1,2,1,1,2,2),'preview_ignores_original':(1,1,2,0,0,0,0),
        'check_unused_fields_body':(1,0,1,0,0,2,2),'put_unused_body':(0,0,2,0,0,0,2),
        'delete_unused_body':(0,0,3,0,0,0,1),'wrong_hwm':(0,0,1,0,1,0,0),
        'wrong_fields':(0,0,2,0,0,1,0),'inflated_fields':(0,0,3,0,0,2,0)}
    for name,case in cases.items():
        expected=specification.expected(case)
        sequence=['SelectCase-'+str(model.CASES.index(case))]
        state=next(destination for action,destination in specification.transitions(specification.initial) if action==sequence[0])
        while state.phase!='done':
            transitions=list(specification.transitions(state))
            if len(transitions)!=1: raise AssertionError('nondeterministic case witness')
            action,state=transitions[0];sequence.append(action)
        report['completed_witnesses'][name]=helper.witness(specification,graph,sequence,
            lambda s,case=case,expected=expected:s.case==case and s.phase=='done' and s.result==expected)
    controls=[]
    for fault,expected in model.FAULTS.items():
        mutant=model.StatePreparation(fault);failure,_=helper.explore(mutant,max_states)
        if failure['status']!='COUNTEREXAMPLE' or failure.get('property')!=expected: raise AssertionError('insensitive state mutant '+fault)
        helper.replay(mutant,failure['trace'],expected)
        controls.append({'fault':fault,'input_subset':'recorded model bounds','trace_sha256':helper.digest(failure['trace']),**failure})
    return {'schema':'fireweed-api007-state-preparation-formal-v1','status':'PASS','baseline':[report],
        'negative_controls':controls,'qualification':{'finite_abstract_safety':'recorded bounds only',
        'production_mapping':'documentary_unexecuted','public_backend_correspondence':'not run',
        'excluded':['current work/selection index integrity','concurrent heads','cleanup/ownership arithmetic',
                    'fairness/deadlines','hash and serializer implementation','whole heap/provider durability']},
        'production_mappings':[{'symbols':['AdmittedGroupStateAuthority::validate','group_authority::load_group_with_fields','state_source::validate_presence'],
          'tests':['group_state_bindings_cover_siblings_collections_empty_groups_and_owner','source_state_preparation_respects_metadata_and_fields_only_knowledge','native_state_preparation_source_cache_local','native_state_preparation_source_cache_real_s3'],
          'status':'documentary_unexecuted'}]}

def main(max_states,path,root):
    try:
        helper.validate_loaded_sources()
        before = helper.provenance(root)
        report = run(max_states)
        helper.validate_loaded_sources()
        if before != helper.provenance(root):
            raise ValueError('source changed during state authority verification')
        report['provenance'] = before
        report['provenance_sha256'] = helper.digest(before)
        report['loaded_sources'] = dict(helper.LOADED_SOURCES)
        helper.write_report(path,report)
        print(helper.canonical({'status':'PASS','models':1,'negative_controls':len(report['negative_controls'])}))
        return 0
    except (ValueError,AssertionError,helper.SearchLimit,OSError,subprocess.CalledProcessError) as error:
        failure = {'schema':'fireweed-api007-state-preparation-formal-v1','status':'FAIL','error':str(error)}
        helper.write_report(path,failure)
        print(helper.canonical(failure))
        return 1
