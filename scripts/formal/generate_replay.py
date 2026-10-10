#!/usr/bin/env python3
"""Generate/validate deterministic action traces from a complete finite graph."""
import argparse
from collections import defaultdict
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys

from replay_model import State, SCHEMA, BOUNDS, BODY, FINGERPRINT, actions, observation, successors, validate_state

MODEL_PATH = Path(__file__).with_name('replay_model.py')


def graph(limit=50000):
    states, ids, parent, edges = [State()], {State(): 0}, [None], []
    for source, state in enumerate(states):
        for action, target, result in actions(state):
            validate_state(target)
            if target not in ids:
                if len(states) >= limit:
                    raise ValueError(f'incomplete model graph: {limit} state limit')
                ids[target] = len(states)
                states.append(target)
                parent.append((source, action))
            edges.append((source, action, ids[target], result))
    return states, ids, parent, edges


def path(parent, node):
    actions_ = []
    while parent[node] is not None:
        previous, action = parent[node]
        actions_.append(action)
        node = previous
    return list(reversed(actions_))


def semantic_class(state, action):
    # Semantic classes are declared selection criteria, not SUT edge exhaustiveness.
    req = int(action.split(':')[1]) if action.startswith(('Replay:', 'LoseResponse:')) else None
    return (action, state.owner, state.reopened, bool(len(state.history) > state.applied),
            bool(req is not None and state.lost & (1 << req)))


REQUIRED = [
    ['Handoff', 'ConcurrentMixedFence', 'DrainApply', 'Replay:0', 'Claim:1'],
    ['Reopen', 'ConcurrentMixedFence', 'DrainApply', 'Replay:0', 'Claim:1'],
    ['Claim:0', 'LoseResponse:0', 'Handoff', 'StaleClaim:0', 'DrainApply', 'Replay:0'],
    ['ConcurrentClaims', 'LoseResponse:0', 'Handoff', 'DrainApply', 'Replay:0', 'Replay:1'],
    ['Reopen', 'StaleClaim:0', 'Handoff', 'StaleClaim:1'],
    ['Handoff', 'StaleClaim:0', 'Reopen', 'StaleClaim:1'],
    ['Claim:0', 'LoseResponse:0', 'Reopen', 'StaleClaim:0', 'Replay:0'],
    ['Claim:0', 'DrainApply', 'Reopen', 'Replay:0'],
]


def generate(profile='smoke', limit=50000):
    states, ids, parent, edges = graph(limit)
    selected, classes = {}, set()
    for source, action, dest, _ in edges:
        key = semantic_class(states[source], action)
        if profile == 'edges' or key not in classes:
            sequence = tuple(path(parent, source) + [action])
            selected.setdefault(sequence, set()).add((source, action, dest))
            classes.add(key)
    for sequence in REQUIRED:
        selected.setdefault(tuple(sequence), set())
    exported, traces, used, intended = {}, [], set(), set()
    for sequence, targets in selected.items():
        candidates = {State()}
        steps = []
        for action in sequence:
            transitions, following = [], set()
            for before in sorted(candidates, key=lambda s: ids[s]):
                choices = successors(before, action)
                if not choices:
                    raise ValueError(f'alternative branch has no continuation for {sequence}')
                for after, result in choices:
                    transitions.append({'before': ids[before], 'after': ids[after], 'result': result})
                    following.add(after)
                    used.update((ids[before], ids[after]))
            steps.append({'action': action, 'transitions': transitions})
            candidates = following
        trace_id = hashlib.sha256(json.dumps(sequence, separators=(',', ':')).encode()).hexdigest()[:16]
        target_ids = [f'{a}|{b}|{c}' for a, b, c in sorted(targets)]
        intended.update(target_ids)
        traces.append({'id': trace_id, 'steps': steps, 'intended_edges': target_ids})
    for node in sorted(used):
        exported[str(node)] = {'model': asdict(states[node]), 'observation': observation(states[node])}
    return {'schema': SCHEMA, 'model_sha256': hashlib.sha256(MODEL_PATH.read_bytes()).hexdigest(),
            'bounds': BOUNDS, 'fingerprint_body': BODY, 'fingerprint': FINGERPRINT,
            'graph': {'complete': True, 'states': len(states), 'edges': len(edges)},
            'selection': {'profile': profile, 'semantic_classes': len(classes),
                          'intended_edges': len(intended), 'traces': len(traces),
                          'sut_edge_coverage': 'must be measured; intended coverage is not witnessed coverage'},
            'initial': 0, 'states': exported, 'traces': traces}


def decode_state(data):
    data = dict(data)
    data['issued'] = tuple(tuple(r) for r in data['issued'])
    data['history'] = tuple(tuple(r) for r in data['history'])
    state = State(**data)
    validate_state(state)
    return state


def validate_corpus(corpus):
    if corpus['schema'] != SCHEMA or corpus['bounds'] != BOUNDS or corpus['fingerprint'] != FINGERPRINT:
        raise ValueError('schema, bounds or body fingerprint mismatch')
    if corpus['model_sha256'] != hashlib.sha256(MODEL_PATH.read_bytes()).hexdigest():
        raise ValueError('stale model source identity')
    if not corpus['graph']['complete'] or not corpus['traces']:
        raise ValueError('incomplete or empty corpus')
    states = {int(key): decode_state(value['model']) for key, value in corpus['states'].items()}
    if states.get(corpus['initial']) != State():
        raise ValueError('incorrect initial state')
    for key, state in states.items():
        if corpus['states'][str(key)]['observation'] != observation(state):
            raise ValueError(f'corrupted independent observation for state {key}')
    inverse = {state: key for key, state in states.items()}
    if len(inverse) != len(states):
        raise ValueError('duplicate model state identity')
    trace_ids = set()
    for trace in corpus['traces']:
        if not trace['steps'] or trace['id'] in trace_ids:
            raise ValueError('empty trace or duplicate trace identity')
        trace_ids.add(trace['id'])
        reachable = {corpus['initial']}
        for step in trace['steps']:
            expected = []
            for before in sorted(reachable):
                for after, result in successors(states[before], step['action']):
                    if after not in inverse:
                        raise ValueError('missing model successor identity')
                    expected.append({'before': before, 'after': inverse[after], 'result': result})
            if not expected or step['transitions'] != expected:
                raise ValueError(f'invalid transitions for {step["action"]}')
            reachable = {edge['after'] for edge in expected}
    if corpus['selection']['traces'] != len(corpus['traces']):
        raise ValueError('trace count mismatch')
    # The corpus is a deterministic, independently derived artifact. Recompute
    # selection as well as transitions so forged coverage metadata, state IDs,
    # omitted cases and extra unreachable states cannot pass validation.
    profile = corpus['selection']['profile']
    if profile not in ('smoke', 'edges'):
        raise ValueError('unknown selection profile')
    if json.loads(json.dumps(corpus)) != json.loads(json.dumps(generate(profile))):
        raise ValueError('corpus differs from canonical complete model selection')
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--validate', type=Path)
    parser.add_argument('--profile', choices=('smoke', 'edges'), default='smoke')
    parser.add_argument('--max-states', type=int, default=50000)
    args = parser.parse_args()
    try:
        if args.validate:
            corpus = json.loads(args.validate.read_text())
        else:
            corpus = generate(args.profile, args.max_states)
        validate_corpus(corpus)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(corpus, sort_keys=True, separators=(',', ':')) + '\n')
        print(json.dumps({'status': 'PASS', 'graph': corpus['graph'], 'selection': corpus['selection']}, sort_keys=True))
        return 0
    except (ValueError, KeyError, TypeError, AssertionError) as error:
        print(json.dumps({'status': 'FAIL', 'error': str(error)}))
        return 1


if __name__ == '__main__':
    sys.exit(main())
