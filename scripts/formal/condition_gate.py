#!/usr/bin/env python3
"""Finite positive-condition safety gate; documentary backend mappings only."""
import hashlib
from pathlib import Path
import subprocess
import sys
import types

# Reuse the reviewed publication runner's source-capture, graph/trace, provenance
# and target-only atomic output utilities without changing its model or runner.
helper_path = Path(__file__).resolve().with_name('check_api007.py')
helper_bytes = helper_path.read_bytes()
helper = types.ModuleType('condition_gate_helpers')
helper.__file__ = str(helper_path)
sys.modules[helper.__name__] = helper
exec(compile(helper_bytes, str(helper_path), 'exec'), helper.__dict__)
helper.LOADED_SOURCES[str(helper_path)] = hashlib.sha256(helper_bytes).hexdigest()
model_path = helper_path.with_name('condition_models.py')
model_bytes = model_path.read_bytes()
model = types.ModuleType('condition_models')
model.__file__ = str(model_path)
sys.modules[model.__name__] = model
exec(compile(model_bytes, str(model_path), 'exec'), model.__dict__)
helper.LOADED_SOURCES[str(model_path)] = hashlib.sha256(model_bytes).hexdigest()


def run(max_states=100000):
    baselines = []
    for configuration in model.CONFIGURATIONS:
        specification = model.Conditions(configuration)
        report, graph = helper.explore(specification, max_states)
        if report['status'] != 'PASS' or not report['complete']:
            raise AssertionError('condition baseline failure: ' + helper.canonical(report))
        states, edges, _ = graph
        if not any(action != 'Observe' for row in edges for action, _ in row):
            raise AssertionError('zero non-observation edges')
        report['graph_sha256'] = helper.digest({'states': [helper.render(s) for s in states], 'edges': edges})
        report['configuration_sha256'] = helper.digest({
            'configuration': configuration, 'bounds': specification.bounds,
            'assumptions': specification.assumptions,
        })
        start = ['InvokeEqual', 'ReadCut', 'SampleFreshTlease']
        finish = ['PublishReceipt', 'ResolveOriginal']
        state_change = 'AdvanceStateVersion' if configuration[1] == 'present' else 'RecreateState'
        committed = lambda s: s.invocation == 1 and s.observations[-1][1] == 'committed'
        state_failure = 'version-conflict' if configuration[1] == 'present' else 'already-exists'
        rejected = lambda s: s.invocation == 1 and s.observations[-1][1] == state_failure
        version_conflict = lambda s: s.invocation == 1 and s.observations[-1][1] == 'version-conflict'
        stale = lambda s: s.invocation == 1 and s.observations[-1][1] == 'stale-lease'
        witnesses = [
            ('success', start + finish, committed),
            ('wrong_token', ['WrongToken'] + start + finish, stale),
            ('wrong_work_version', ['AdvanceWorkVersion'] + start + finish, version_conflict),
            ('wrong_version_and_token', ['AdvanceWorkVersion', 'WrongToken'] + start + finish, stale),
            ('wrong_version_and_expiry', ['AdvanceWorkVersion', 'ExactExpiry'] + start + finish, stale),
            ('exact_expiry', ['ExactExpiry'] + start + finish, stale),
            ('state_predicate_failure', [state_change] + start + finish, rejected),
            ('time_passage_after_sample', start + ['ExactExpiry'] + finish, committed),
            ('epoch_cut_discard', start + ['Fence', 'DiscardStaleCut', 'ReadCut', 'SampleFreshTlease'] + finish, committed),
            ('head_cut_discard', start + [state_change, 'DiscardStaleCut', 'ReadCut', 'SampleFreshTlease'] + finish, rejected),
            ('equal_replay_after_expiry', start + finish + ['ExactExpiry', 'InvokeEqual', 'ResolveOriginal'],
             lambda s: s.invocation == 2 and s.observations[-1][1] == 'committed'),
            ('equal_replay_after_rows_change', start + finish + [state_change, 'InvokeEqual', 'ResolveOriginal'],
             lambda s: s.invocation == 2 and s.observations[-1][1] == 'committed'),
            ('changed_body_replay', start + finish + ['InvokeChanged', 'ResolveOriginal'],
             lambda s: s.invocation == 2 and s.observations[-1][1] == 'request-conflict'),
            ('rejected_original_replay', [state_change] + start + finish + ['InvokeEqual', 'ResolveOriginal'],
             lambda s: s.invocation == 2 and s.observations[-1][1] == state_failure),
        ]
        if configuration[1] == 'present':
            witnesses.append(('delete_recreate_hwm', ['DeleteState', 'RecreateState'] + start + finish, rejected))
            witnesses.append(('version_check_missing', ['DeleteState'] + start + finish,
                              lambda s: s.invocation == 1 and s.observations[-1][1] == 'not-found'))
            witnesses.append(('missing_original_replay', ['DeleteState'] + start + finish + ['InvokeEqual', 'ResolveOriginal'],
                              lambda s: s.invocation == 2 and s.observations[-1][1] == 'not-found'))
        report['completed_witnesses'] = {
            name: helper.witness(specification, graph, actions, predicate)
            for name, actions, predicate in witnesses
        }
        baselines.append(report)
    controls = []
    for fault, expected in model.FAULTS.items():
        configuration = ('mixed', 'present') if fault == 'partial_rejection' else ('checks-only', 'present')
        specification = model.Conditions(configuration, fault)
        report, _ = helper.explore(specification, max_states)
        if report['status'] != 'COUNTEREXAMPLE' or report.get('property') != expected:
            raise AssertionError('insensitive condition negative control: ' + fault)
        helper.replay(specification, report['trace'], expected)
        controls.append({'fault': fault, 'configuration': configuration,
                         'trace_sha256': helper.digest(report['trace']), **report})
    return {
        'schema': 'fireweed-api007-conditions-formal-v1', 'status': 'PASS',
        'baseline': baselines, 'negative_controls': controls,
        'production_mappings': [{
            'status': 'documentary_unexecuted',
            'properties': model.Conditions.properties,
            'source': 'crates/fireweed-engine/src/queue_planner.rs', 'symbol': 'apply_group',
            'tests': [
                'condition_checks_preserve_max_versions_and_rollback_late_failure',
                'condition_checks_skip_large_payloads_and_keep_checked_unique_owners',
                'condition_checks_use_metadata_admission_and_preserve_unique_owners',
                'native_store_runs_identical_contract_local',
                'native_store_runs_identical_contract_real_s3',
            ],
        }],
        'qualification': {
            'finite_abstract_safety': 'established only within recorded bounds and assumptions',
            'production_mapping': 'documentary_unexecuted', 'public_backend_correspondence': 'not run',
            'excluded': ['negative claim predicates', 'full Snorri rejection ordering',
                         'independent concurrent same-ID callers', 'key/hash collisions',
                         'FP4 administration', 'memory/capacity bounds', 'fairness/progress/deadlines',
                         'cache integrity', 'physical provider durability'],
        },
    }


def main(max_states, path, root):
    try:
        helper.validate_loaded_sources()
        before = helper.provenance(root)
        report = run(max_states)
        helper.validate_loaded_sources()
        if before != helper.provenance(root):
            raise ValueError('source changed during condition verification')
        report['provenance'] = before
        report['provenance_sha256'] = helper.digest(before)
        report['loaded_sources'] = dict(helper.LOADED_SOURCES)
        helper.write_report(path, report)
        print(helper.canonical({'status': 'PASS', 'models': len(report['baseline']),
                                'negative_controls': len(report['negative_controls'])}))
        return 0
    except (ValueError, AssertionError, helper.SearchLimit, OSError, subprocess.CalledProcessError) as error:
        failure = {'schema': 'fireweed-api007-conditions-formal-v1', 'status': 'FAIL', 'error': str(error)}
        if path is not None:
            helper.write_report(path, failure)
        print(helper.canonical(failure))
        return 1
