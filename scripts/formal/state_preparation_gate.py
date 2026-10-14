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
    work_report, work_controls = run_work(max_states)
    return {'schema':'fireweed-api007-state-preparation-formal-v1','status':'PASS','baseline':[report, work_report],
        'work_negative_controls': work_controls,
        'negative_controls':controls,'qualification':{'finite_abstract_safety':'recorded bounds only',
        'production_mapping':'documentary_unexecuted','public_backend_correspondence':'not run',
        'excluded':['complete work/selection index integrity','concurrent heads','cleanup/ownership arithmetic',
                    'fairness/deadlines','hash and serializer implementation','whole heap/provider durability']},
        'production_mappings':[{'symbols':['AdmittedGroupStateAuthority::validate','group_authority::load_group_with_fields','state_source::validate_presence'],
          'tests':['group_state_bindings_cover_siblings_collections_empty_groups_and_owner','source_state_preparation_respects_metadata_and_fields_only_knowledge','native_state_preparation_source_cache_local','native_state_preparation_source_cache_real_s3'],
          'status':'documentary_unexecuted'}]}

def run_work(max_states):
    specification = model.WorkPreparation()
    report, graph = helper.explore(specification, max_states)
    if report['status'] != 'PASS' or not report['complete']:
        raise AssertionError('incomplete work baseline')
    states, edges, _ = graph
    report['graph_sha256'] = helper.digest({'states':[helper.render(s) for s in states], 'edges':edges})
    cases = {
        'original_ignores_current_work': (0,1,2,1,1,2,2),
        'preview_checks_work': (1,1,1,0,0,1,0),
        'check_unused_fields_body': (0,0,0,0,0,2,2),
        'selected_unused_body': (0,0,1,0,0,0,2),
        'foreign_work': (0,0,0,1,0,0,0),
        'lease_or_version_mismatch': (0,0,0,0,1,0,0),
        'same_size_field_corruption': (0,0,1,0,0,1,0),
        'inflated_field_before_fetch': (0,0,1,0,0,2,0),
        'required_body_exact': (0,0,2,0,0,0,0),
        'required_body_same_size_corruption': (0,0,2,0,0,0,1),
        'required_body_inflation_before_fetch': (0,0,2,0,0,0,2),
    }
    report['completed_witnesses'] = {}
    for name, case in cases.items():
        expected = specification.expected(case)
        sequence = ['SelectWork-'+str(model.WORK_CASES.index(case))]
        state = next(destination for action,destination in specification.transitions(specification.initial) if action == sequence[0])
        while state.phase != 'done':
            transitions = list(specification.transitions(state))
            if len(transitions) != 1: raise AssertionError('nondeterministic work witness')
            action,state = transitions[0]; sequence.append(action)
        report['completed_witnesses'][name] = helper.witness(specification,graph,sequence,
            lambda s,case=case,expected=expected:s.case == case and s.phase == 'done' and s.result == expected)
    controls = []
    for fault, expected in model.WORK_FAULTS.items():
        mutant = model.WorkPreparation(fault)
        failure, _ = helper.explore(mutant,max_states)
        if failure['status'] != 'COUNTEREXAMPLE' or failure.get('property') != expected:
            raise AssertionError('insensitive work mutant '+fault)
        helper.replay(mutant,failure['trace'],expected)
        controls.append({'fault':fault,'trace_sha256':helper.digest(failure['trace']),**failure})
    report['production_mapping'] = {
        'status':'documentary_unexecuted',
        'symbols':['ColdStateTracker::initialization_genesis','ColdGroupStateTracker::initialize_work','WorkSet::observe','WorkSet::descriptor',
            'work_source::validate_presence','work_source::validate_payload_metadata','WorkDescriptor::verify_fields','WorkDescriptor::verify_body'],
        'tests':['selected_fold_handles_replace_edit_preserve_delete_recreate_and_lease_metadata',
            'initialization_errors_are_sticky_and_never_seal_a_repaired_tracker',
            'work_initialization_rejects_foreign_address_and_replaced_genesis_token',
            'initialized_empty_source_checks_group_key_membership_and_continuation_owner',
            'source_work_metadata_and_selected_field_corruption_fail_before_pack_planning'],
    }
    return report, controls

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
        print(helper.canonical({'status':'PASS','models':len(report['baseline']),'negative_controls':len(report['negative_controls'])+len(report['work_negative_controls'])}))
        return 0
    except (ValueError,AssertionError,helper.SearchLimit,OSError,subprocess.CalledProcessError) as error:
        failure = {'schema':'fireweed-api007-state-preparation-formal-v1','status':'FAIL','error':str(error)}
        helper.write_report(path,failure)
        print(helper.canonical(failure))
        return 1
