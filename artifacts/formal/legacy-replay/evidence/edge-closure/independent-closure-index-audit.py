#!/usr/bin/env python3
"""Independently verify every formerly missing edge's new physical witness."""
import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path
import sys


def sha(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(chunk)
    return result.hexdigest()


def audit(root, results):
    previous_dir = root / 'docs/helix/04-build/evidence/all-edge-replay'
    manifest = json.loads((previous_dir / 'manifest.json').read_text())
    old_path = previous_dir / 'report.json.gz'
    old_meta = manifest['artifacts']['report.json.gz']
    assert sha(old_path) == old_meta['retained_sha256']
    assert old_path.stat().st_size == old_meta['retained_bytes']
    old_raw = gzip.decompress(old_path.read_bytes())
    old_hash = hashlib.sha256(old_raw).hexdigest()
    assert old_hash == old_meta['original_sha256'] == 'a83979c040ce45fceb591c5f818644d937b73e35c90032128b404d9e84fb798f'
    assert len(old_raw) == old_meta['original_bytes']
    old = json.loads(old_raw)
    del old_raw
    assert old['status'] == old['execution']['status'] == 'PASS'
    old_matched = set(old['execution']['matched_edges'])
    old_model = old['execution']['model_sha256']
    assert len(old_matched) == len(old['execution']['matched_edges']) == 16995
    del old

    current_path = results / 'exhaustive.json'
    current = json.loads(current_path.read_text())
    index_path = results / 'closed-edge-witness-index.json'
    index = json.loads(index_path.read_text())
    final_path = results / 'independent-final-audit.json'
    final = json.loads(final_path.read_text())
    assert current['status'] == current['execution']['status'] == final['status'] == index['status'] == 'PASS'
    assert current['s3_fixture']['cleanup_confirmed'] is True
    assert index['schema'] == 'fireweed-previously-unwitnessed-edge-index-v1'
    assert index['previous_report_sha256'] == old_hash
    current_hash = sha(current_path)
    assert index['current_report_sha256'] == current_hash == final['artifacts']['report']['sha256']
    observation_hash = sha(current['observations'])
    assert index['observations_sha256'] == current['observations_sha256'] == observation_hash == final['artifacts']['observations']['sha256']
    assert index['model_sha256'] == old_model == current['execution']['model_sha256'] == final['unchanged_model_sha256']

    sys.path.insert(0, str(root / 'scripts/formal'))
    from replay_model import State, actions, observation
    states, ids, graph = [State()], {State(): 0}, set()
    for source, state in enumerate(states):
        for action, target, _ in actions(state):
            if target not in ids:
                ids[target] = len(states)
                states.append(target)
            graph.add(f'{source}|{action}|{ids[target]}')
    assert len(states) == 6386 and len(graph) == 20913
    assert old_matched <= graph
    missing = graph - old_matched
    assert len(missing) == 3918
    assert set(index['witnesses']) == missing
    assert set(current['execution']['matched_edges']) == graph
    assert index['previous_unwitnessed'] == index['closed'] == len(missing) and index['remaining'] == 0

    # Reconstruct all current paths directly from model actions and observations.
    # Derive the earliest witness index independently, never trusting matched_edges.
    first_witnesses, witnessed, action_counts = {}, set(), Counter()
    for case_index, case in enumerate(current['execution']['cases']):
        assert case['index'] == case_index and case['status'] == 'PASS'
        state_id = 0
        for step_index, step in enumerate(case['steps']):
            assert step['step'] == step_index and step['status'] == 'PASS'
            action = step['action']
            matches = [target for label, target, result in actions(states[state_id])
                       if label == action and result == step['result']
                       and observation(target) == step['observation']]
            assert len(matches) == 1, (case_index, step_index, 'no unique actual path')
            target_id = ids[matches[0]]
            edge = f'{state_id}|{action}|{target_id}'
            assert step['matched_edges'] == [edge]
            assert step['compatible_before'] == [state_id] and step['compatible_after'] == [target_id]
            if edge in missing and edge not in first_witnesses:
                first_witnesses[edge] = {'case_index': case_index, 'case_id': case['trace'],
                    'base_trace': case['base_trace'], 'schedule': case['schedule'],
                    'step': step_index, 'action': action}
                action_counts[action] += 1
            witnessed.add(edge)
            state_id = target_id
    assert witnessed == graph
    assert first_witnesses == index['witnesses']
    assert all(witness['schedule'] == 'request-1-first' for witness in first_witnesses.values())
    assert action_counts['ConcurrentClaims'] == 26 and sum(action_counts.values()) == 3918
    return {'schema': 'fireweed-independent-closure-index-audit-v1', 'status': 'PASS',
        'disposition': 'All 3918 prior unwitnessed edges independently tied to actual current case/step observations',
        'previous_report_sha256': old_hash, 'current_report_sha256': current_hash,
        'observations_sha256': observation_hash, 'model_sha256': old_model,
        'index_sha256': sha(index_path), 'independent_final_audit_sha256': sha(final_path),
        'previously_witnessed': len(old_matched), 'closed': len(first_witnesses), 'remaining': 0,
        'concurrent_edges_closed': action_counts['ConcurrentClaims'], 'downstream_edges_closed': 3918 - action_counts['ConcurrentClaims'],
        'closed_action_counts': dict(sorted(action_counts.items())),
        'current_graph_edges_reconstructed': len(witnessed), 'all_witness_schedules': 'request-1-first',
        'previous_compressed_and_original_hashes_verified': True, 'audit_script_sha256': sha(__file__)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--results', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.root, args.results)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
