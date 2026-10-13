#!/usr/bin/env python3
"""Bounded original authority gate; mappings are documentary, unexecuted."""
import hashlib
from pathlib import Path
import subprocess
import sys
import types

helper_path = Path(__file__).resolve().with_name('check_api007.py')
helper_bytes = helper_path.read_bytes()
helper = types.ModuleType('original_gate_helpers')
helper.__file__ = str(helper_path)
sys.modules[helper.__name__] = helper
exec(compile(helper_bytes, str(helper_path), 'exec'), helper.__dict__)
helper.LOADED_SOURCES[str(helper_path)] = hashlib.sha256(helper_bytes).hexdigest()
model_path = helper_path.with_name('original_models.py')
model_bytes = model_path.read_bytes()
model = types.ModuleType('original_models')
model.__file__ = str(model_path)
sys.modules[model.__name__] = model
exec(compile(model_bytes, str(model_path), 'exec'), model.__dict__)
helper.LOADED_SOURCES[str(model_path)] = hashlib.sha256(model_bytes).hexdigest()

def run(max_states=100000):
    specification = model.Originals()
    report, graph = helper.explore(specification, max_states)
    if report['status'] != 'PASS' or not report['complete']:
        raise AssertionError('original authority baseline failure: ' + helper.canonical(report))
    states, edges, _ = graph
    if not any(action != 'Observe' for row in edges for action, _ in row):
        raise AssertionError('zero non-observation edges')
    report['graph_sha256'] = helper.digest({'states':[helper.render(s) for s in states], 'edges':edges})
    report['completed_witnesses'] = {}
    witness_cases = {
        'exact_original':(True,True,0,0,0,0,0,0,0,True),
        'fresh_absence':(False,False,0,0,0,0,0,0,0,True),
        'phantom_cache':(False,True,0,0,0,0,0,0,0,True),
        'deleted_cache':(True,False,0,0,0,0,0,0,0,True),
        'wrong_fp_matches_caller':(True,True,0,1,0,1,0,0,0,True),
        'wrong_format':(True,True,1,0,0,0,0,0,0,True),
        'conflict_before_expiry_and_payload':(True,True,0,0,0,1,1,2,1,True),
        'unsupported_expiry':(True,True,0,0,0,0,1,0,0,True),
        'oversized_before_payload':(True,True,0,0,0,0,0,1,0,True),
        'same_length_wrong_outcome':(True,True,0,0,0,0,0,0,1,True),
        'incomplete_or_mismatched_prefix':(True,True,0,0,0,0,0,0,0,False),
    }
    for name, case in witness_cases.items():
        actions = ['SelectCase-' + str(model.CASES.index(case)), 'ReadCheckedPrefix']
        phase = 'metadata' if case[-1] else 'finish'
        if phase == 'metadata':
            actions.append('ReadCacheMetadata')
            phase, _ = specification.metadata_plan(case)
        if phase == 'payload':
            actions += ['ReadOpaqueOutcome','VerifyExactOutcome']
        expected = specification.expected(case)
        actions += ['CompleteClassifications','ReturnUnknown' if expected == 'unknown' else 'PublishFreshSibling']
        report['completed_witnesses'][name] = helper.witness(specification,graph,actions,
            lambda s, expected=expected, case=case: s.case == case and s.phase == 'done' and s.result == expected and s.publications == (0 if expected == 'unknown' else 1))
    controls = []
    for fault, expected in model.FAULTS.items():
        mutant = model.Originals(fault)
        failure, _ = helper.explore(mutant,max_states)
        if failure['status'] != 'COUNTEREXAMPLE' or failure.get('property') != expected:
            raise AssertionError('insensitive original authority control: '+fault+' '+helper.canonical(failure))
        helper.replay(mutant,failure['trace'],expected)
        controls.append({'fault':fault, 'trace_sha256':helper.digest(failure['trace']), **failure})
    return {'schema':'fireweed-api007-originals-formal-v1','status':'PASS','baseline':[report],
        'negative_controls':controls,'qualification':{'finite_abstract_safety':'recorded bounds only',
        'production_mapping':'documentary_unexecuted','public_backend_correspondence':'not run',
        'excluded':['global work/state/index integrity','same-ID concurrent callers','post-snapshot concurrency',
                    'fair progress/deadlines','wire/cryptographic implementation','memory/provider durability']},
        'production_mappings':[{'symbols':['ColdOriginalTracker','load_raw_authoritative'],
          'tests':['native_batch_original_authority_rejects_corrupt_cache_local','native_batch_original_authority_rejects_corrupt_cache_real_s3'],
          'status':'documentary_unexecuted'}]}

def main(max_states,path,root):
    try:
        helper.validate_loaded_sources()
        before = helper.provenance(root)
        report = run(max_states)
        helper.validate_loaded_sources()
        if before != helper.provenance(root):
            raise ValueError('source changed during original authority verification')
        report['provenance'] = before
        report['provenance_sha256'] = helper.digest(before)
        report['loaded_sources'] = dict(helper.LOADED_SOURCES)
        helper.write_report(path,report)
        print(helper.canonical({'status':'PASS','models':1,'negative_controls':len(report['negative_controls'])}))
        return 0
    except (ValueError,AssertionError,helper.SearchLimit,OSError,subprocess.CalledProcessError) as error:
        failure = {'schema':'fireweed-api007-originals-formal-v1','status':'FAIL','error':str(error)}
        helper.write_report(path,failure)
        print(helper.canonical(failure))
        return 1
