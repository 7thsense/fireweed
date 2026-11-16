#!/usr/bin/env python3
"""Bounded retained-owner queue gate; mappings are documentary, unexecuted."""
import hashlib
from pathlib import Path
import subprocess
import sys
import types

helper_path = Path(__file__).resolve().with_name('check_api007.py')
helper_bytes = helper_path.read_bytes()
helper = types.ModuleType('owner_gate_helpers')
helper.__file__ = str(helper_path)
sys.modules[helper.__name__] = helper
exec(compile(helper_bytes, str(helper_path), 'exec'), helper.__dict__)
helper.LOADED_SOURCES[str(helper_path)] = hashlib.sha256(helper_bytes).hexdigest()
model_path = helper_path.with_name('owner_models.py')
model_bytes = model_path.read_bytes()
# check.py imports owner_models normally; a failed exec here must not shadow it.
model = types.ModuleType('owner_gate_models')
model.__file__ = str(model_path)
sys.modules[model.__name__] = model
exec(compile(model_bytes, str(model_path), 'exec'), model.__dict__)
helper.LOADED_SOURCES[str(model_path)] = hashlib.sha256(model_bytes).hexdigest()

ENTER = ['Acquire0', 'Process0', 'Granted0', 'SkewDone0', 'Fence0', 'Confirm0']
TAKEOVER = ['Acquire1', 'Process1', 'Granted1', 'SkewDone1', 'Fence1', 'Confirm1']
# name: (scenario, ordered actions, predicate on the final state)
WITNESSES = {
    'healthy_write_then_read': ('base', ENTER + ['Publish:a0', 'ReadTake0', 'ReadEval0', 'ReadAck0'],
        lambda s: s.acked == ('a',) and s.reads == 1 and s.owners[0].phase == 'serving'),
    'lost_publish_reply_exits_and_rebuilds': ('base', ENTER + ['PublishLostReply:a0', 'Exit0'] + ENTER
            + ['ReplayAck:a0'],
        lambda s: s.log == (('a', 1),) and s.acked == ('a',) and s.fence == 2),
    'superseded_fence_cannot_commit': ('base', ENTER + ['Tick', 'Tick', 'Exit0'] + TAKEOVER
            + ['Publish:a1'],
        lambda s: s.log == (('a', 2),) and s.acked == ('a',) and s.owners[0].phase == 'out'),
    'stalled_reader_is_not_acked': ('base', ENTER + ['ReadTake0', 'Tick', 'Tick', 'ReadEval0'],
        lambda s: s.owners[0].read in ('eval', 'stale') and s.now >= s.owners[0].deadline),
    'renewal_moves_the_deadline': ('renew', ENTER + ['Renew0', 'Process0', 'Renewed0'],
        lambda s: s.owners[0].deadline > 2 and s.owners[0].phase == 'serving'),
    'lost_renewal_reply_keeps_the_deadline': ('lost', ENTER + ['Renew0', 'Process0', 'LoseReply0'],
        lambda s: s.owners[0].deadline == 2 and s.lost == 1 and s.owners[0].phase == 'serving'),
    'crash_discards_the_projection': ('crash', ENTER + ['Publish:a0', 'Crash0'],
        lambda s: s.owners[0].proj == () and s.owners[0].phase == 'out' and s.crashes == 1),
}


def settled(state, horizon):
    """Quiescent, or out of modeled time: the horizon is a bound, not a deadlock."""
    return state.now >= horizon or all(
        h.phase in ('out', 'serving') and not h.read and not h.answer and not h.unresolved
        for h in state.owners)


def stuck_states(states, edges, horizon):
    """States with no path to a settled state; guards must not hide oracles."""
    reverse = [[] for _ in states]
    for source, row in enumerate(edges):
        for _, dest in row:
            reverse[dest].append(source)
    reachable = {index for index, state in enumerate(states) if settled(state, horizon)}
    todo = list(reachable)
    while todo:
        for source in reverse[todo.pop()]:
            if source not in reachable:
                reachable.add(source)
                todo.append(source)
    return len(states) - len(reachable)


def run(max_states=100000):
    baselines, witnesses = [], {}
    for scenario in model.SCENARIOS:
        specification = model.Owner(scenario=scenario)
        report, graph = helper.explore(specification, max_states)
        if report['status'] != 'PASS' or not report['complete']:
            raise AssertionError('owner baseline failure: ' + helper.canonical(report))
        states, edges, _ = graph
        if not any(action != 'Observe' for row in edges for action, _ in row):
            raise AssertionError('zero non-observation edges')
        if stuck_states(states, edges, model.SCENARIOS[scenario]['horizon']):
            raise AssertionError('owner protocol has stuck states in ' + scenario)
        if set(model.FAULTS.values()) != set(specification.properties):
            raise AssertionError('every owner property needs a faulty control')
        report['scenario'] = scenario
        report['graph_sha256'] = helper.digest({'states': [helper.render(s) for s in states], 'edges': edges})
        for name, (wanted, actions, finish) in WITNESSES.items():
            if wanted == scenario:
                witnesses[name] = helper.witness(specification, graph, actions, finish)
        baselines.append(report)
    if set(witnesses) != set(WITNESSES):
        raise AssertionError('every owner witness needs a scenario')
    controls = []
    for fault, expected in model.FAULTS.items():
        mutant = model.Owner(fault)
        failure, _ = helper.explore(mutant, max_states)
        if failure['status'] != 'COUNTEREXAMPLE' or failure.get('property') != expected:
            raise AssertionError('insensitive owner control: ' + fault + ' ' + helper.canonical(failure))
        helper.replay(mutant, failure['trace'], expected)
        controls.append({'fault': fault, 'scenario': mutant.scenario,
                         'trace_sha256': helper.digest(failure['trace']), **failure})
    baselines[0]['completed_witnesses'] = witnesses
    return {'schema': 'fireweed-api007-owner-formal-v1', 'status': 'PASS', 'baseline': baselines,
        'negative_controls': controls, 'qualification': {'finite_abstract_safety': 'recorded bounds only',
        'production_mapping': 'documentary_unexecuted', 'public_backend_correspondence': 'not run',
        'excluded': ['multiple queues on one handle', 'independent same-identity callers',
                     'fair progress', 'clock drift beyond the one-tick skew allowance',
                     'wire/cryptographic implementation', 'memory/provider durability',
                     'virtual machine freezes that stop the lease clock']},
        'production_mappings': [{'symbols': ['Tenure', 'QueueLoop'], 'status': 'documentary_unexecuted'}]}


def main(max_states, path, root):
    try:
        helper.validate_loaded_sources()
        before = helper.provenance(root)
        report = run(max_states)
        helper.validate_loaded_sources()
        if before != helper.provenance(root):
            raise ValueError('source changed during owner verification')
        report['provenance'] = before
        report['provenance_sha256'] = helper.digest(before)
        report['loaded_sources'] = dict(helper.LOADED_SOURCES)
        helper.write_report(path, report)
        print(helper.canonical({'status': 'PASS', 'models': len(report['baseline']), 'negative_controls': len(report['negative_controls'])}))
        return 0
    except (ValueError, AssertionError, helper.SearchLimit, OSError, subprocess.CalledProcessError) as error:
        failure = {'schema': 'fireweed-api007-owner-formal-v1', 'status': 'FAIL', 'error': str(error)}
        helper.write_report(path, failure)
        print(helper.canonical(failure))
        return 1
