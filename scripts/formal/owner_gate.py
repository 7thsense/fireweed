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
model = types.ModuleType('owner_models')
model.__file__ = str(model_path)
sys.modules[model.__name__] = model
exec(compile(model_bytes, str(model_path), 'exec'), model.__dict__)
helper.LOADED_SOURCES[str(model_path)] = hashlib.sha256(model_bytes).hexdigest()

FIRST_WRITE = ['OpenEmpty', 'BeginWrite:a', 'AcquireFence', 'CatchUp', 'Plan']
LOCKED_READ = ['ReadHead', 'ReadLock', 'ReadCatchUp', 'ServeLocked']
WITNESSES = {
    'healthy_write_and_read': (FIRST_WRITE + ['PublishAccepted', 'ApplyAndAck', 'BeginWrite:b', 'Plan',
                                              'PublishAccepted', 'ApplyAndAck', 'ReadHead', 'ServeFast'],
        lambda s: s.projection == ('a', 'b') and s.reads == 1 and not s.read_phase and s.fence == 1),
    'foreign_write_caught_up_by_read': (['OpenEmpty', 'ForeignFence', 'ForeignAppend'] + LOCKED_READ,
        lambda s: s.projection == ('f',) and s.reads == 1 and not s.read_phase and s.held == 0),
    'superseded_fence_replans': (FIRST_WRITE + ['PublishRejected', 'AcquireFence', 'CatchUp', 'Plan',
                                                'PublishAccepted', 'ApplyAndAck'],
        lambda s: s.acknowledged == ('a',) and s.log == ('f', 'a') and s.phase == 'idle'),
    'lost_reply_resolves_present': (FIRST_WRITE + ['PublishLostReply', 'ResolveFence', 'ResolveCatchUp',
                                                   'ResolvePresent'],
        lambda s: s.log == ('a',) and s.acknowledged == ('a',) and s.fence == 2 and s.phase == 'idle'),
    'late_publish_cannot_land_after_absence': (FIRST_WRITE + ['PublishInFlight', 'ResolveFence',
        'ResolveCatchUp', 'ResolveAbsent', 'Plan', 'PublishAccepted', 'ApplyAndAck', 'InFlightExpires'],
        lambda s: s.log == ('a',) and s.acknowledged == ('a',) and not s.inflight and s.phase == 'idle'),
    'crash_rebuilds_from_genesis': (FIRST_WRITE + ['PublishAccepted', 'ApplyAndAck', 'Crash', 'OpenEmpty']
                                    + LOCKED_READ,
        lambda s: s.projection == ('a',) and s.reads == 1 and not s.read_phase and s.crashes == 1),
    'clean_shutdown_reopens_trusted': (FIRST_WRITE + ['PublishAccepted', 'ApplyAndAck', 'Shutdown',
                                                      'OpenTrusted', 'ReadHead', 'ServeFast'],
        lambda s: s.projection == ('a',) and s.reads == 1 and not s.read_phase and not s.sidecar),
    'apply_failure_poisons_and_rebuilds': (FIRST_WRITE + ['PublishAccepted', 'ApplyFails', 'PoisonRebuild',
                                                          'RebuildCatchUp', 'ReplayOriginal'],
        lambda s: s.acknowledged == ('a',) and s.projection == ('a',) and not s.poisoned and not s.tainted),
}


def settled(state):
    return state.phase in ('idle', 'closed') and not state.operation and not state.unresolved


def stuck_states(states, edges):
    """States with no path to a settled state; guards must not hide oracles."""
    reverse = [[] for _ in states]
    for source, row in enumerate(edges):
        for _, dest in row:
            reverse[dest].append(source)
    reachable = {index for index, state in enumerate(states) if settled(state)}
    todo = list(reachable)
    while todo:
        for source in reverse[todo.pop()]:
            if source not in reachable:
                reachable.add(source)
                todo.append(source)
    return len(states) - len(reachable)


def run(max_states=100000):
    specification = model.Owner()
    report, graph = helper.explore(specification, max_states)
    if report['status'] != 'PASS' or not report['complete']:
        raise AssertionError('owner baseline failure: ' + helper.canonical(report))
    states, edges, _ = graph
    if not any(action != 'Observe' for row in edges for action, _ in row):
        raise AssertionError('zero non-observation edges')
    if stuck_states(states, edges):
        raise AssertionError('owner protocol has stuck states')
    if set(model.FAULTS.values()) != set(specification.properties):
        raise AssertionError('every owner property needs a faulty control')
    report['graph_sha256'] = helper.digest({'states': [helper.render(s) for s in states], 'edges': edges})
    report['completed_witnesses'] = {name: helper.witness(specification, graph, actions, finish)
                                     for name, (actions, finish) in WITNESSES.items()}
    controls = []
    for fault, expected in model.FAULTS.items():
        mutant = model.Owner(fault)
        failure, _ = helper.explore(mutant, max_states)
        if failure['status'] != 'COUNTEREXAMPLE' or failure.get('property') != expected:
            raise AssertionError('insensitive owner control: ' + fault + ' ' + helper.canonical(failure))
        helper.replay(mutant, failure['trace'], expected)
        controls.append({'fault': fault, 'trace_sha256': helper.digest(failure['trace']), **failure})
    return {'schema': 'fireweed-api007-owner-formal-v1', 'status': 'PASS', 'baseline': [report],
        'negative_controls': controls, 'qualification': {'finite_abstract_safety': 'recorded bounds only',
        'production_mapping': 'documentary_unexecuted', 'public_backend_correspondence': 'not run',
        'excluded': ['multiple queues on one handle', 'independent same-identity callers',
                     'fair progress/deadlines', 'wire/cryptographic implementation',
                     'memory/provider durability', 'torn projection files beyond the trust rule']},
        'production_mappings': [{'symbols': ['OwnedQueue'], 'status': 'documentary_unexecuted'}]}


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
        print(helper.canonical({'status': 'PASS', 'models': 1, 'negative_controls': len(report['negative_controls'])}))
        return 0
    except (ValueError, AssertionError, helper.SearchLimit, OSError, subprocess.CalledProcessError) as error:
        failure = {'schema': 'fireweed-api007-owner-formal-v1', 'status': 'FAIL', 'error': str(error)}
        helper.write_report(path, failure)
        print(helper.canonical(failure))
        return 1
