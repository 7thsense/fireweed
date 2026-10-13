#!/usr/bin/env python3
"""Bounded domain authority gate; mappings are documentary, unexecuted."""
import hashlib
from pathlib import Path
import subprocess
import sys
import types

helper_path = Path(__file__).resolve().with_name('check_api007.py')
helper_bytes = helper_path.read_bytes()
helper = types.ModuleType('domain_gate_helpers')
helper.__file__ = str(helper_path)
sys.modules[helper.__name__] = helper
exec(compile(helper_bytes, str(helper_path), 'exec'), helper.__dict__)
helper.LOADED_SOURCES[str(helper_path)] = hashlib.sha256(helper_bytes).hexdigest()
model_path = helper_path.with_name('domain_models.py')
model_bytes = model_path.read_bytes()
model = types.ModuleType('domain_models')
model.__file__ = str(model_path)
sys.modules[model.__name__] = model
exec(compile(model_bytes, str(model_path), 'exec'), model.__dict__)
helper.LOADED_SOURCES[str(model_path)] = hashlib.sha256(model_bytes).hexdigest()

def run(max_states=100000):
    specification = model.Domains()
    report, graph = helper.explore(specification,max_states)
    if report['status'] != 'PASS' or not report['complete']: raise AssertionError('incomplete domain baseline')
    states, edges, _ = graph
    if not any(action == 'ReadNativeOriginalMetadata' for row in edges for action,_ in row): raise AssertionError('no physical lookup abstraction')
    report['graph_sha256'] = helper.digest({'states':[helper.render(s) for s in states],'edges':edges})
    report['completed_witnesses'] = {}
    cases = {'insert_original':(7,0,0,0,0),'claim_original':(7,1,1,0,0),'commit_original':(7,2,2,0,0),
        'bound_absence':(0,0,0,0,0),'foreign_absence':(0,0,1,0,0),'foreign_present':(7,0,1,0,0),'foreign_literal':(7,0,0,0,1)}
    for name,case in cases.items():
        sequence = ['SelectCase-'+str(model.CASES.index(case)),'FoldExactSourceReceipt']
        expected = specification.expected(case)
        sequence += ['RefuseForeignProof'] if expected == 'unknown' else ['ReadNativeOriginalMetadata','ReturnSourceClassification']
        report['completed_witnesses'][name] = helper.witness(specification,graph,sequence,
            lambda s,case=case,expected=expected: s.case == case and s.phase == 'done' and s.result == expected and s.reads == (0 if expected == 'unknown' else 1))
    controls = []
    for fault,expected in model.FAULTS.items():
        mutant = model.Domains(fault);failure,_ = helper.explore(mutant,max_states)
        if failure['status'] != 'COUNTEREXAMPLE' or failure.get('property') != expected: raise AssertionError('insensitive domain mutant '+fault)
        helper.replay(mutant,failure['trace'],expected)
        controls.append({'fault':fault,'trace_sha256':helper.digest(failure['trace']),**failure})
    return {'schema':'fireweed-api007-domains-formal-v1','status':'PASS','baseline':[report],
        'negative_controls':controls,'qualification':{'finite_abstract_safety':'recorded bounds only',
        'production_mapping':'documentary_unexecuted','public_backend_correspondence':'not run',
        'excluded':['cache metadata/opaque corruption covered separately','current work/state/index integrity',
                    'head concurrency covered separately','fair progress/deadlines','wire/cryptographic implementation','RAM/provider durability']},
        'production_mappings':[{'symbols':['ColdOriginalTracker::new_request','AdmittedOriginalPosition::validate_domain'],
          'tests':['single_original_domains_bind_presence_absence_and_exact_admission',
                   'native_single_original_authority_rejects_corrupt_cache_local','native_single_original_authority_rejects_corrupt_cache_real_s3'],
          'status':'documentary_unexecuted'}]}

def main(max_states,path,root):
    try:
        helper.validate_loaded_sources()
        before = helper.provenance(root)
        report = run(max_states)
        helper.validate_loaded_sources()
        if before != helper.provenance(root):
            raise ValueError('source changed during domain authority verification')
        report['provenance'] = before
        report['provenance_sha256'] = helper.digest(before)
        report['loaded_sources'] = dict(helper.LOADED_SOURCES)
        helper.write_report(path,report)
        print(helper.canonical({'status':'PASS','models':1,'negative_controls':len(report['negative_controls'])}))
        return 0
    except (ValueError,AssertionError,helper.SearchLimit,OSError,subprocess.CalledProcessError) as error:
        failure = {'schema':'fireweed-api007-domains-formal-v1','status':'FAIL','error':str(error)}
        helper.write_report(path,failure)
        print(helper.canonical(failure))
        return 1
