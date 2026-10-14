#!/usr/bin/env python3
"""Bounded batch continuation gate; mappings are documentary, unexecuted."""
import hashlib
from pathlib import Path
import subprocess
import sys
import types

helper_path = Path(__file__).resolve().with_name('check_api007.py')
helper_bytes = helper_path.read_bytes()
helper = types.ModuleType('continuation_gate_helpers')
helper.__file__ = str(helper_path)
sys.modules[helper.__name__] = helper
exec(compile(helper_bytes, str(helper_path), 'exec'), helper.__dict__)
helper.LOADED_SOURCES[str(helper_path)] = hashlib.sha256(helper_bytes).hexdigest()
model_path = helper_path.with_name('continuation_models.py')
model_bytes = model_path.read_bytes()
model = types.ModuleType('continuation_models')
model.__file__ = str(model_path)
sys.modules[model.__name__] = model
exec(compile(model_bytes, str(model_path), 'exec'), model.__dict__)
helper.LOADED_SOURCES[str(model_path)] = hashlib.sha256(model_bytes).hexdigest()

def run(max_states=100000):
    specification = model.Continuation()
    report, graph = helper.explore(specification, max_states)
    if report['status'] != 'PASS' or not report['complete']:
        raise AssertionError('batch continuation baseline failure: ' + helper.canonical(report))
    states, edges, _ = graph
    if not any(action != 'Observe' for row in edges for action, _ in row):
        raise AssertionError('zero non-observation edges')
    report['graph_sha256'] = helper.digest({'states':[helper.render(s) for s in states], 'edges':edges})
    report['completed_witnesses'] = {}
    witness_paths = {
        case: (['Choose:'+case, 'AcquireFresh', 'ResolveCovered'],
               lambda s, case=case:s.phase=='done' and s.case==case and s.terminal=='Covered')
        for case in ('empty','suffix','deleted-recreated','missing-projection','behind-projection','retry-valid')
    }
    witness_paths['retry-valid'] = (
        ['Choose:retry-valid','AcquireFresh','CoveredReplayFailure','AcquireFresh','ResolveCovered'],
        lambda s:s.phase=='done' and s.case=='retry-valid' and s.terminal=='Covered' and s.retried)
    witness_paths.update({
        case: (['Choose:'+case, 'RejectKnownRegression' if case in ('rewind','vanished','retry-rewind') else
                'RejectScope' if case in ('wrong-source','wrong-scope') else 'RejectSuffix'],
               lambda s, case=case:s.phase=='done' and s.case==case and s.terminal=='Unknown')
        for case in ('rewind','vanished','retry-rewind','wrong-source','wrong-scope','duplicate','poison')
    })
    for name,(actions,finish) in witness_paths.items():
        report['completed_witnesses'][name] = helper.witness(specification,graph,actions,finish)
    controls = []
    for fault, expected in model.FAULTS.items():
        mutant = model.Continuation(fault)
        failure, _ = helper.explore(mutant,max_states)
        if failure['status'] != 'COUNTEREXAMPLE' or failure.get('property') != expected:
            raise AssertionError('insensitive batch continuation control: '+fault+' '+helper.canonical(failure))
        helper.replay(mutant,failure['trace'],expected)
        controls.append({'fault':fault, 'trace_sha256':helper.digest(failure['trace']), **failure})
    return {'schema':'fireweed-api007-continuation-formal-v1','status':'PASS','baseline':[report],
        'negative_controls':controls,'qualification':{'finite_abstract_safety':'recorded bounds only',
        'production_mapping':'documentary_unexecuted','public_backend_correspondence':'not run',
        'excluded':['global work/state/index integrity','unbounded concurrent callers',
                    'fair progress/deadlines','wire/cryptographic implementation','memory/provider durability']},
        'production_mappings':[{'symbols':['RecoveryTask::execute_scanned','resolve_submission_after','ColdOriginalTracker::resume','ColdGroupStateTracker::resume'],
          'tests':['batch_continuation_empty_suffix_preserves_historical_epoch_and_rebuilds_missing_projection','submission_resolution_does_not_retry_failed_continuation_into_fenced_absence'],
          'status':'documentary_unexecuted'}]}


def main(max_states,path,root):
    try:
        helper.validate_loaded_sources()
        before = helper.provenance(root)
        report = run(max_states)
        helper.validate_loaded_sources()
        if before != helper.provenance(root):
            raise ValueError('source changed during batch continuation verification')
        report['provenance'] = before
        report['provenance_sha256'] = helper.digest(before)
        report['loaded_sources'] = dict(helper.LOADED_SOURCES)
        helper.write_report(path,report)
        print(helper.canonical({'status':'PASS','models':1,'negative_controls':len(report['negative_controls'])}))
        return 0
    except (ValueError,AssertionError,helper.SearchLimit,OSError,subprocess.CalledProcessError) as error:
        failure = {'schema':'fireweed-api007-continuation-formal-v1','status':'FAIL','error':str(error)}
        helper.write_report(path,failure)
        print(helper.canonical(failure))
        return 1
