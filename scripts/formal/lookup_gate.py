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
model_path = helper_path.with_name('lookup_models.py')
model_bytes = model_path.read_bytes()
model = types.ModuleType('lookup_models')
model.__file__ = str(model_path)
sys.modules[model.__name__] = model
exec(compile(model_bytes, str(model_path), 'exec'), model.__dict__)
helper.LOADED_SOURCES[str(model_path)] = hashlib.sha256(model_bytes).hexdigest()

def run(max_states=100000):
    specification = model.Lookups()
    report, graph = helper.explore(specification, max_states)
    if report['status'] != 'PASS' or not report['complete']: raise AssertionError('incomplete lookup baseline')
    states, edges, _ = graph
    report['graph_sha256'] = helper.digest({'states':[helper.render(s) for s in states], 'edges':edges})
    report['completed_witnesses'] = {}
    cases = {'empty':((),(),0,0,0), 'empty_foreign':((),(),0,1,0),
        'aliases':((0,1,0),(0,1,0),1,0,0), 'ordered_present':((0,1),(0,1),3,0,0), 'truncated':((0,1),(0,),3,0,0),
        'reordered':((0,1),(1,0),3,0,0), 'source_duplicate':((0,),(0,),1,0,1)}
    for name, case in cases.items():
        expected = specification.expected(case)
        sequence = ['SelectCase-'+str(model.CASES.index(case)), 'RejectAuthority' if expected is None else 'ReturnOrderedResults']
        report['completed_witnesses'][name] = helper.witness(specification, graph, sequence,
            lambda s,case=case,expected=expected: s.case == case and s.phase == 'done' and s.accepted == (expected is not None) and (expected is None or s.result == expected))
    controls = []
    for fault, expected in model.FAULTS.items():
        mutant = model.Lookups(fault)
        failure, _ = helper.explore(mutant, max_states)
        if failure['status'] != 'COUNTEREXAMPLE' or failure.get('property') != expected: raise AssertionError('insensitive lookup mutant '+fault)
        helper.replay(mutant, failure['trace'], expected)
        controls.append({'fault':fault, 'trace_sha256':helper.digest(failure['trace']), **failure})
    return {'schema':'fireweed-api007-lookups-formal-v1', 'status':'PASS', 'baseline':[report],
        'negative_controls':controls, 'qualification':{'finite_abstract_safety':'recorded bounds only',
        'production_mapping':'documentary_unexecuted', 'public_backend_correspondence':'not run',
        'excluded':['cache metadata/opaque corruption covered separately', 'current work/state/index integrity',
        'concurrent head changes', 'fairness/deadlines', 'wire/cryptographic implementation', 'RAM/provider durability']},
        'production_mappings':[{'symbols':['ColdOriginalTracker::new_lookup','AdmittedOriginalAuthority::validate_lookup'],
        'tests':['lookup_authority_validates_whole_vector_before_native_reads',
                 'lookup_aliases_bind_whole_order_empty_scope_and_duplicate_source_poison'],
        'status':'documentary_unexecuted'}]}

def main(max_states,path,root):
    try:
        helper.validate_loaded_sources()
        before = helper.provenance(root)
        report = run(max_states)
        helper.validate_loaded_sources()
        if before != helper.provenance(root):
            raise ValueError('source changed during lookup authority verification')
        report['provenance'] = before
        report['provenance_sha256'] = helper.digest(before)
        report['loaded_sources'] = dict(helper.LOADED_SOURCES)
        helper.write_report(path,report)
        print(helper.canonical({'status':'PASS','models':1,'negative_controls':len(report['negative_controls'])}))
        return 0
    except (ValueError,AssertionError,helper.SearchLimit,OSError,subprocess.CalledProcessError) as error:
        failure = {'schema':'fireweed-api007-lookups-formal-v1','status':'FAIL','error':str(error)}
        helper.write_report(path,failure)
        print(helper.canonical(failure))
        return 1
