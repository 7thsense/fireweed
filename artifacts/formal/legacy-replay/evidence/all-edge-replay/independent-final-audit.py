#!/usr/bin/env python3
"""Read-only independent reconstruction of canonical replay execution evidence."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import statistics
import subprocess
import sys


def digest(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def audit(args):
    sys.path.insert(0, str(args.root / 'scripts/formal'))
    from generate_replay import validate_corpus
    report = json.loads(args.report.read_text())
    assert report['schema'] == 'fireweed-generated-replay-run-v1'
    assert report['status'] == 'PASS' and report['exit_code'] == 0
    assert report['profile'] == 'edges'
    artifacts = {}
    for key in ('corpus', 'observations', 'progress', 'log'):
        actual = digest(report[key])
        assert actual == report[key + '_sha256'], (key, 'artifact hash mismatch')
        artifacts[key] = {'path': report[key], 'sha256': actual}
    before, after = report['provenance_before'], report['provenance_after']
    assert before['head'] == after['head']
    assert before['source_sha256'] == after['source_sha256']
    actual_head = subprocess.check_output(['git', '-C', str(args.root), 'rev-parse', 'HEAD'], text=True).strip()
    assert actual_head == after['head'], 'current HEAD mismatch'
    for name, expected in after['source_sha256'].items():
        assert digest(args.root / name) == expected, (name, 'current source mismatch')
    corpus = json.loads(Path(report['corpus']).read_text())
    validate_corpus(corpus)
    assert corpus['graph'] == {'complete': True, 'states': 6386, 'edges': 20913}
    traces = corpus['traces']
    assert len(traces) == 17002 and sum(len(t['steps']) for t in traces) == 127208
    detail = report['execution']
    assert detail == json.loads(Path(report['observations']).read_text()), 'embedded execution copy'
    assert detail['schema'] == 'fireweed-model-replay-evidence-v1' and detail['status'] == 'PASS'
    assert detail['corpus_sha256'] == report['corpus_sha256']
    assert detail['selection'] == corpus['selection'] and detail['model_graph'] == corpus['graph']
    assert detail['model_sha256'] == corpus['model_sha256']
    assert detail['started'] == detail['attempted'] == detail['passed'] == len(detail['cases']) == len(traces)
    assert detail['workers'] == 8 and detail['all_selected_traces_executed'] is True
    log = Path(report['log']).read_text()
    expected_tests = {
        'generated_replay_claim_tail_guard_rejects_unproven_suffixes',
        'generated_replay_decoder_rejects_malformed_empty_and_stale_corpora',
        'generated_replay_oracle_rejects_corrupted_observations_and_branch_switching',
        'generated_replay_storage_generation_scan_is_world_scoped',
        'generated_s3_turso_model_traces_match_every_observation',
    }
    passed_tests = set(re.findall(r'test turso_compose::generated_replay_tests::(\w+) \.\.\. ok', log))
    assert passed_tests == expected_tests
    summaries = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;.*?finished in ([\d.]+)s', log)
    assert len(summaries) == 1 and tuple(map(int, summaries[0][:3])) == (5, 0, 0)
    matched, intended, all_graph_edges = set(), set(), set()
    case_edges, actions, branches, reopen_counts = {}, Counter(), Counter(), Counter()
    elapsed = []
    for index, (case, trace) in enumerate(zip(detail['cases'], traces)):
        assert case['index'] == index and case['trace'] == trace['id'] and case['status'] == 'PASS'
        assert case['error'] is None and case['intended_edges'] == trace['intended_edges']
        assert len(case['steps']) == len(trace['steps'])
        current, witnessed = {corpus['initial']}, set()
        elapsed.append(case['elapsed_ms'])
        for step_index, (observed, predicted) in enumerate(zip(case['steps'], trace['steps'])):
            where = (index, step_index, predicted['action'])
            assert observed['step'] == step_index and observed['action'] == predicted['action'] and observed['status'] == 'PASS', where
            assert set(observed['compatible_before']) == current, (where, 'predecessor set')
            following, edges = set(), set()
            for edge in predicted['transitions']:
                key = f"{edge['before']}|{predicted['action']}|{edge['after']}"
                all_graph_edges.add(key)
                if edge['before'] in current and edge['result'] == observed['result'] and corpus['states'][str(edge['after'])]['observation'] == observed['observation']:
                    following.add(edge['after'])
                    edges.add(key)
            assert following, (where, 'no compatible model path')
            assert following == set(observed['compatible_after']), (where, 'successor set')
            assert edges == set(observed['matched_edges']) and len(edges) == len(observed['matched_edges']), (where, 'matched edges')
            models = [corpus['states'][str(state)]['model'] for state in following]
            assert len({model['applied'] for model in models}) == 1
            model = models[0]
            physical = [item for item in observed['diagnostics'] if item['operation'] == 'Observe']
            assert len(physical) == 1, (where, 'physical observation multiplicity')
            physical = physical[0]
            positions = physical['durable_positions']
            assert len(positions) == len(model['history']) + 1, (where, 'durable history length')
            assert [p['sequence'] for p in positions] == list(range(len(positions))), (where, 'durable sequence')
            assert all(p['queue'] == {'tenant_id': 'formal', 'queue_id': 'claims'} for p in positions), (where, 'durable queue')
            assert positions[0]['backend_epoch'] == 0
            assert [p['backend_epoch'] for p in positions[1:]] == [record[2] for record in model['history']], (where, 'publication epochs')
            assert physical['projection_before'] == physical['projection_after'] == positions[model['applied']], (where, 'exact side-effect-free applied frontier')
            reopen = [item for item in observed['diagnostics'] if item['operation'] == 'Reopen']
            if predicted['action'] == 'Reopen':
                assert len(reopen) == 1
                reopen = reopen[0]
                prefixes = {corpus['states'][str(state)]['model']['applied'] for state in current}
                assert len(prefixes) == 1
                assert reopen['pre_close'] == reopen['post_close'] == positions[next(iter(prefixes))], (where, 'close advanced paused apply')
                assert reopen['post_open'] == positions[-1], (where, 'reopen missing tail')
                assert reopen['tail_repaired'] == (reopen['pre_close'] != reopen['post_open'])
                assert reopen['same_namespace'] is True and reopen['same_projection_file'] is True
                reopen_counts['tail_repaired' if reopen['tail_repaired'] else 'retained_state'] += 1
            else:
                assert not reopen
            if predicted['action'] == 'ConcurrentClaims':
                branches[str([response['item'] for response in observed['result']['responses']])] += 1
            actions[predicted['action']] += 1
            current = following
            witnessed.update(edges)
        intended.update(trace['intended_edges'])
        matched.update(witnessed)
        case_edges[index] = witnessed
    assert len(intended) == len(all_graph_edges) == 20913 and intended == all_graph_edges
    assert matched == set(detail['matched_edges']) and len(matched) == len(detail['matched_edges'])
    assert matched <= intended and sum(actions.values()) == 127208
    exhaustive = matched == all_graph_edges
    assert detail['sut_graph_edge_exhaustive'] == exhaustive
    assert report['coverage'] == {'intended': len(intended), 'matched': len(matched), 'intended_unwitnessed': len(intended - matched), 'graph_edge_exhaustive': exhaustive}
    events = [json.loads(line) for line in Path(report['progress']).read_text().splitlines()]
    assert len(events) == len(traces) and {e['index'] for e in events} == set(range(len(traces)))
    accumulated = set()
    for completion, event in enumerate(events, 1):
        case = detail['cases'][event['index']]
        accumulated.update(case_edges[event['index']])
        assert event['schema'] == 'fireweed-replay-progress-v1' and event['trace'] == case['trace']
        assert event['status'] == 'PASS' and event['total'] == len(traces)
        assert event['completed'] == event['passed'] == completion
        assert event['elapsed_ms'] == case['elapsed_ms'] and event['matched_edges'] == len(accumulated)
    fixture = report['s3_fixture']
    cas_path = Path(fixture['native_cas_evidence'])
    cas = json.loads(cas_path.read_text())
    assert cas['status'] == 'passed' and cas['endpoint'] == fixture['endpoint']
    assert cas['native_atomic_conditional_create'] is True and cas['native_atomic_conditional_update'] is True
    assert cas['sequential_create_only']['first_status'] == 200 and cas['sequential_create_only']['second_status'] == 412
    assert cas['sequential_create_only']['winner_body_preserved'] is True
    assert sorted(item['status'] for item in cas['two_writer_create_only_race']['writer_results']) == [200, 412]
    assert cas['conditional_update']['if_match_current_status'] == 200 and cas['conditional_update']['if_match_stale_status'] == 412
    assert fixture['console_enabled'] is False
    artifacts['native_cas'] = {'path': str(cas_path), 'sha256': digest(cas_path)}
    artifacts['report'] = {'path': str(args.report), 'sha256': digest(args.report)}
    if args.rustfs_binary:
        assert digest(args.rustfs_binary) == fixture['binary_sha256'], 'RustFS binary identity'
    latency = args.report.parent / 'independent-prefix-latency-audit.json'
    if latency.exists():
        prior = json.loads(latency.read_text())
        new_progress_bytes = Path(report['progress']).read_bytes()
        assert hashlib.sha256(new_progress_bytes[:prior['new_progress_snapshot_bytes']]).hexdigest() == prior['new_progress_snapshot_sha256']
        assert prior['unchanged_corpus_sha256'] == report['corpus_sha256']
    return {
        'schema': 'fireweed-independent-all-edge-audit-v1',
        'status': 'PASS', 'disposition': 'APPROVE within declared bounded backend replay scope',
        'source_head': actual_head, 'current_source_files_verified': len(after['source_sha256']),
        'canonical_graph': corpus['graph'], 'traces': len(traces), 'steps': sum(actions.values()),
        'coverage': report['coverage'], 'workers': detail['workers'],
        'exact_rust_tests_verified': sorted(expected_tests), 'rust_test_elapsed_seconds': float(summaries[0][3]),
        'case_elapsed_ms': {'min': min(elapsed), 'median': statistics.median(elapsed), 'max': max(elapsed)},
        'action_counts': dict(actions), 'concurrent_claim_item_orders': dict(branches),
        'reopen_counts': dict(reopen_counts), 'progress_events_reconciled': len(events),
        'rustfs_binary_sha256': fixture['binary_sha256'], 'rustfs_binary_verified': bool(args.rustfs_binary),
        'artifacts': artifacts, 'audit_script_sha256': digest(__file__),
        'limits': ['one-process ephemeral control plane', 'real local RustFS and Turso, not production-cloud S3', 'backend-level composition, not facade routing or API-007 queue pipeline', 'graceful reopen, not process-kill recovery', 'simulated client nonreceipt', 'matched edge coverage is not graph-exhaustive', 'no universal refinement or numeric production deadline claim'],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--rustfs-binary', type=Path)
    args = parser.parse_args()
    result = audit(args)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
