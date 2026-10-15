#!/usr/bin/env python3
"""Bounded source-plan/preparation gate; documentary Rust correspondence only."""
import hashlib
from pathlib import Path
import subprocess
import sys
import types

helper_path = Path(__file__).resolve().with_name('check_api007.py')
helper_bytes = helper_path.read_bytes()
helper = types.ModuleType('source_group_helpers')
helper.__file__ = str(helper_path)
sys.modules[helper.__name__] = helper
exec(compile(helper_bytes, str(helper_path), 'exec'), helper.__dict__)
helper.LOADED_SOURCES[str(helper_path)] = hashlib.sha256(helper_bytes).hexdigest()
model_path = helper_path.with_name('source_group_models.py')
model_bytes = model_path.read_bytes()
model = types.ModuleType('source_group_models')
model.__file__ = str(model_path)
sys.modules[model.__name__] = model
exec(compile(model_bytes, str(model_path), 'exec'), model.__dict__)
helper.LOADED_SOURCES[str(model_path)] = hashlib.sha256(model_bytes).hexdigest()

WITNESSES = {
    'mixed_check_corruption_ignored': ('mixed_check_corrupt','same','absent',False,'none',False),
    'state_metadata_only_check': ('state_check','same','absent',False,'field',False),
    'state_check_cannot_retarget': ('state_check','retarget','absent',False,'none',False),
    'foreign_equal_head_fields': ('mixed','cut_aba','absent',False,'none',False),
    'selected_overcount': ('selected_overcount','same','absent',False,'none',False),
    'complete_count': ('state_short','same','release',False,'none',False),
    'complete_chain': ('state_incomplete','same','release',False,'none',False),
    'future_sequence': ('future_sequence','same','absent',False,'none',False),
    'future_body': ('future_body','same','absent',False,'none',False),
    'future_state': ('future_state','same','absent',False,'none',False),
    'unique_release': ('mixed','same','release',False,'none',False),
    'unique_reclaim': ('mixed','same','reclaim',False,'none',False),
    'unique_swap': ('swap','same','swap',False,'none',False),
    'surviving_owner_conflict': ('mixed','same','conflict',False,'none',False),
    'missing_release_unknown': ('mixed','same','missing_release',False,'none',False),
    'business_rejection_precedes_release': ('mixed','same','missing_release',True,'none',False),
    'lease_expires_during_probe': ('mixed','same','missing_release',False,'none',True),
    'admission_failure': ('mixed','same','absent',False,'budget',False),
    'partial_cancel': ('mixed','same','release',False,'cancel',False),
    'probe_failure': ('mixed','same','release',False,'unique',False),
}

def run(max_states=100000):
    specification = model.SourceGroup()
    report, graph = helper.explore(specification, max_states)
    if report['status'] != 'PASS' or not report['complete']: raise AssertionError('incomplete source-group baseline')
    states, edges, _ = graph
    report['graph_sha256'] = helper.digest({'states':[helper.render(s) for s in states], 'edges':edges})
    report['completed_witnesses'] = {}
    for name, case in WITNESSES.items():
        first = 'SelectCase-'+str(model.CASES.index(case))
        report['completed_witnesses'][name] = helper.witness(specification, graph, [first],
            lambda s, case=case: s.case == case and s.phase == 'done' and s.result == model.expected(case))
    controls = []
    for fault, expected in model.FAULTS.items():
        mutant = model.SourceGroup(fault)
        failure, _ = helper.explore(mutant, max_states)
        if failure['status'] != 'COUNTEREXAMPLE' or failure.get('property') != expected:
            raise AssertionError('insensitive source-group mutant '+fault)
        helper.replay(mutant, failure['trace'], expected)
        controls.append({'fault':fault, 'trace_sha256':helper.digest(failure['trace']), **failure})
    publication = model.SourceGroupPublication()
    publication_report, publication_graph = helper.explore(publication, max_states)
    if publication_report['status'] != 'PASS' or not publication_report['complete']:
        raise AssertionError('incomplete publication boundary baseline')
    p_states, p_edges, _ = publication_graph
    publication_report['graph_sha256'] = helper.digest({'states':[helper.render(s) for s in p_states], 'edges':p_edges})
    publication_report['completed_witnesses'] = {}
    for i, case in enumerate(model.PUBLICATION_CASES):
        publication_report['completed_witnesses'][str(case)] = helper.witness(publication, publication_graph,
            ['SelectPublication-'+str(i)], lambda s, case=case: s.case == case and s.phase == 'done'
                and s.prepared == (case[1] == 'same' and case[2] == 'none'))
    for fault, expected in model.PUBLICATION_FAULTS.items():
        mutant = model.SourceGroupPublication(fault)
        failure, _ = helper.explore(mutant, max_states)
        if failure['status'] != 'COUNTEREXAMPLE' or failure.get('property') != expected:
            raise AssertionError('insensitive publication boundary mutant '+fault)
        helper.replay(mutant, failure['trace'], expected)
        controls.append({'fault':fault, 'trace_sha256':helper.digest(failure['trace']), **failure})
    return {'schema':'fireweed-api007-source-group-formal-v1', 'status':'PASS',
        'baseline':[report, publication_report], 'negative_controls':controls,
        'qualification':{'finite_abstract_safety':'recorded bounds only', 'production_mapping':'documentary_unexecuted',
            'excluded':['Rust refinement','exact allocation arithmetic','canonical decoding and hashes',
                        'publication/CAS','provider durability','performance','public QueueStore activation']},
        'production_mappings':[{'symbols':['load_group_preimage','QueueSourceGroupPreimage::plan','load_group_fields',
                'QueueSourceGroupPlan::encode','QueueSourceGroupPublication::record_for',
                'QueueNativeAcquired::prepare_group_publication'],
            'tests':['fields_cannot_recapture_foreign_cut_then_return_to_original_head',
                     'mixed_work_check_never_reads_another_keys_mutation_fields',
                     'atomic_state_and_next_work_match_shared_full_image_planner',
                     'source_bridge_complete_producer_matches_cold_oracle_real_s3'], 'status':'documentary_unexecuted'}]}

def main(max_states, path, root):
    try:
        helper.validate_loaded_sources()
        before = helper.provenance(root)
        report = run(max_states)
        helper.validate_loaded_sources()
        if before != helper.provenance(root): raise ValueError('source changed during source-group verification')
        report['provenance'] = before
        report['provenance_sha256'] = helper.digest(before)
        report['loaded_sources'] = dict(helper.LOADED_SOURCES)
        helper.write_report(path, report)
        print(helper.canonical({'status':'PASS','models':2,'negative_controls':len(report['negative_controls'])}))
        return 0
    except (ValueError, AssertionError, helper.SearchLimit, OSError, subprocess.CalledProcessError) as error:
        failure = {'schema':'fireweed-api007-source-group-formal-v1','status':'FAIL','error':str(error)}
        helper.write_report(path, failure)
        print(helper.canonical(failure))
        return 1
