#!/usr/bin/env python3
"""Bounded batch handoff gate; mappings are documentary, unexecuted."""
import hashlib
from pathlib import Path
import subprocess
import sys
import types

helper_path = Path(__file__).resolve().with_name('check_api007.py')
helper_bytes = helper_path.read_bytes()
helper = types.ModuleType('handoff_gate_helpers')
helper.__file__ = str(helper_path)
sys.modules[helper.__name__] = helper
exec(compile(helper_bytes, str(helper_path), 'exec'), helper.__dict__)
helper.LOADED_SOURCES[str(helper_path)] = hashlib.sha256(helper_bytes).hexdigest()
model_path = helper_path.with_name('handoff_models.py')
model_bytes = model_path.read_bytes()
model = types.ModuleType('handoff_models')
model.__file__ = str(model_path)
sys.modules[model.__name__] = model
exec(compile(model_bytes, str(model_path), 'exec'), model.__dict__)
helper.LOADED_SOURCES[str(model_path)] = hashlib.sha256(model_bytes).hexdigest()

def run(max_states=100000):
    specification = model.Handoff()
    report, graph = helper.explore(specification, max_states)
    if report['status'] != 'PASS' or not report['complete']:
        raise AssertionError('batch handoff baseline failure: ' + helper.canonical(report))
    states, edges, _ = graph
    if not any(action != 'Observe' for row in edges for action, _ in row):
        raise AssertionError('zero non-observation edges')
    report['graph_sha256'] = helper.digest({'states':[helper.render(s) for s in states], 'edges':edges})
    report['completed_witnesses'] = {}
    witness_paths = {
        'three_fresh_handoffs':(['RecoverFullPrefix','BindDeclaredPosition','PrepareNative','ConditionalPublish','PostRecoverFullPrefix','ResolveOriginal'] +
            ['BindDeclaredPosition','PrepareNative','ConditionalPublish','PostRecoverFullPrefix','ResolveOriginal']*2,
            lambda s:s.phase=='done' and s.group==3 and s.recoveries==4 and not s.foreign and not s.fenced and not s.raced),
        'stale_handoff_falls_back':(['RecoverFullPrefix','ForeignPublication','DiscardStaleHandoff','RecoverFullPrefix','ResolveOriginal'],
            lambda s:s.phase=='done' and s.group==3 and s.foreign),
        'fenced_rejection_reprepares':(['RecoverFullPrefix','PrepareNative','CompetingFence','ConditionalReject','PostRecoverFullPrefix','ReprepareFencedAbsent','ConditionalPublish','ResolveOriginal'],
            lambda s:s.phase=='done' and s.group==3 and s.fenced),
        'non_newer_fence_refuses_resubmission':(['RecoverFullPrefix','PrepareNative','CompetingFence','ConditionalReject','PostReadWithoutNewFence','ReturnUnknown'],
            lambda s:s.phase=='done' and s.group==0 and not any(s.originals) and s.non_newer and not s.foreign and not s.raced),
        'raced_original_replays':(['RecoverFullPrefix','RacedOriginal','DiscardStaleHandoff','RecoverFullPrefix','KnownOriginal','PostRecoverFullPrefix','ResolveOriginal'],
            lambda s:s.phase=='done' and s.group==3 and s.raced),
    }
    for name,(actions,finish) in witness_paths.items():
        report['completed_witnesses'][name] = helper.witness(specification,graph,actions,finish)
    controls = []
    for fault, expected in model.FAULTS.items():
        mutant = model.Handoff(fault)
        failure, _ = helper.explore(mutant,max_states)
        if failure['status'] != 'COUNTEREXAMPLE' or failure.get('property') != expected:
            raise AssertionError('insensitive batch handoff control: '+fault+' '+helper.canonical(failure))
        helper.replay(mutant,failure['trace'],expected)
        controls.append({'fault':fault, 'trace_sha256':helper.digest(failure['trace']), **failure})
    return {'schema':'fireweed-api007-handoff-formal-v1','status':'PASS','baseline':[report],
        'negative_controls':controls,'qualification':{'finite_abstract_safety':'recorded bounds only',
        'production_mapping':'documentary_unexecuted','public_backend_correspondence':'not run',
        'excluded':['global work/state/index integrity','unbounded concurrent callers',
                    'fair progress/deadlines','wire/cryptographic implementation','memory/provider durability']},
        'production_mappings':[{'symbols':['CommitExecution::execute_covered','AdmittedOriginalPosition'],
          'tests':['one_permit_batch_validates_all_siblings_replays_before_fresh_and_finishes_after_caller_drop'],
          'status':'documentary_unexecuted'}]}


def main(max_states,path,root):
    try:
        helper.validate_loaded_sources()
        before = helper.provenance(root)
        report = run(max_states)
        helper.validate_loaded_sources()
        if before != helper.provenance(root):
            raise ValueError('source changed during batch handoff verification')
        report['provenance'] = before
        report['provenance_sha256'] = helper.digest(before)
        report['loaded_sources'] = dict(helper.LOADED_SOURCES)
        helper.write_report(path,report)
        print(helper.canonical({'status':'PASS','models':1,'negative_controls':len(report['negative_controls'])}))
        return 0
    except (ValueError,AssertionError,helper.SearchLimit,OSError,subprocess.CalledProcessError) as error:
        failure = {'schema':'fireweed-api007-handoff-formal-v1','status':'FAIL','error':str(error)}
        helper.write_report(path,failure)
        print(helper.canonical(failure))
        return 1
