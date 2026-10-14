#!/usr/bin/env python3
"""Bounded batch pack_publication gate; mappings are documentary, unexecuted."""
import hashlib
from pathlib import Path
import subprocess
import sys
import types

helper_path = Path(__file__).resolve().with_name('check_api007.py')
helper_bytes = helper_path.read_bytes()
helper = types.ModuleType('pack_publication_gate_helpers')
helper.__file__ = str(helper_path)
sys.modules[helper.__name__] = helper
exec(compile(helper_bytes, str(helper_path), 'exec'), helper.__dict__)
helper.LOADED_SOURCES[str(helper_path)] = hashlib.sha256(helper_bytes).hexdigest()
model_path = helper_path.with_name('pack_publication_models.py')
model_bytes = model_path.read_bytes()
model = types.ModuleType('pack_publication_models')
model.__file__ = str(model_path)
sys.modules[model.__name__] = model
exec(compile(model_bytes, str(model_path), 'exec'), model.__dict__)
helper.LOADED_SOURCES[str(model_path)] = hashlib.sha256(model_bytes).hexdigest()

def run(max_states=100000):
    specification = model.PackPublication()
    report, graph = helper.explore(specification, max_states)
    if report['status'] != 'PASS' or not report['complete']:
        raise AssertionError('batch pack_publication baseline failure: ' + helper.canonical(report))
    states, edges, _ = graph
    if not any(action != 'Observe' for row in edges for action, _ in row):
        raise AssertionError('zero non-observation edges')
    report['graph_sha256'] = helper.digest({'states':[helper.render(s) for s in states], 'edges':edges})
    report['completed_witnesses'] = {}
    witness_paths = {
        case: (['Choose:'+case, 'ConditionalPublication', 'RejectRegression' if case=='regression' else 'AcknowledgeSourceReceipts'],
               lambda s,case=case:s.phase=='done' and s.case==case and s.terminal==('Unknown' if case=='regression' else 'Success'))
        for case in model.CASES
    }
    for name,(actions,finish) in witness_paths.items():
        report['completed_witnesses'][name] = helper.witness(specification,graph,actions,finish)
    controls = []
    for fault, expected in model.FAULTS.items():
        mutant = model.PackPublication(fault)
        failure, _ = helper.explore(mutant,max_states)
        if failure['status'] != 'COUNTEREXAMPLE' or failure.get('property') != expected:
            raise AssertionError('insensitive batch pack_publication control: '+fault+' '+helper.canonical(failure))
        helper.replay(mutant,failure['trace'],expected)
        controls.append({'fault':fault, 'trace_sha256':helper.digest(failure['trace']), **failure})
    return {'schema':'fireweed-api007-pack_publication-formal-v1','status':'PASS','baseline':[report],
        'negative_controls':controls,'qualification':{'finite_abstract_safety':'recorded bounds only',
        'production_mapping':'documentary_unexecuted','public_backend_correspondence':'not run',
        'excluded':['global work/state/index integrity','unbounded concurrent callers',
                    'fair progress/deadlines','wire/cryptographic implementation','memory/provider durability']},
        'production_mappings':[{'symbols':['BatchTask::pack','BatchTask::classify_after','CommitExecution::execute_batch_covered','resolve_submission_after'],
          'tests':['one_permit_batch_validates_all_siblings_replays_before_fresh_and_finishes_after_caller_drop','confirmed_batch_handoff_recovers_head_and_fence_races_local','confirmed_batch_handoff_recovers_head_and_fence_races_real_s3','packed_submission_refreshes_middle_outside_and_fp4_originals_local','packed_submission_refreshes_middle_outside_and_fp4_originals_real_s3','packed_replay_head_advancement_retains_checked_floor_local','packed_replay_head_advancement_retains_checked_floor_real_s3'],
          'status':'documentary_unexecuted'}]}



def main(max_states,path,root):
    try:
        helper.validate_loaded_sources()
        before = helper.provenance(root)
        report = run(max_states)
        helper.validate_loaded_sources()
        if before != helper.provenance(root):
            raise ValueError('source changed during batch pack_publication verification')
        report['provenance'] = before
        report['provenance_sha256'] = helper.digest(before)
        report['loaded_sources'] = dict(helper.LOADED_SOURCES)
        helper.write_report(path,report)
        print(helper.canonical({'status':'PASS','models':1,'negative_controls':len(report['negative_controls'])}))
        return 0
    except (ValueError,AssertionError,helper.SearchLimit,OSError,subprocess.CalledProcessError) as error:
        failure = {'schema':'fireweed-api007-pack_publication-formal-v1','status':'FAIL','error':str(error)}
        helper.write_report(path,failure)
        print(helper.canonical(failure))
        return 1
