#!/usr/bin/env python3
"""Independent reconstruction of scheduled real-S3/Turso replay evidence.

This auditor never trusts reported coverage, compatible states or schedule success.
It reconstructs the graph directly from the unchanged independent model and every
observed compatible path, checks actual backend-poll overlap, then reconciles all
case identities and cumulative progress. It writes only its requested result file.
"""
import argparse
from collections import Counter
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys

MODEL_SHA256 = '980ecc848792ead1da674a2334ace465966fe90169bed5b2f98c0574e0beeb82'
SCHEDULES = ('request-0-first', 'request-1-first')
RUST_TESTS = {
    'generated_replay_claim_tail_guard_rejects_unproven_suffixes',
    'generated_replay_decoder_rejects_malformed_empty_and_stale_corpora',
    'generated_replay_oracle_rejects_corrupted_observations_and_branch_switching',
    'generated_replay_storage_generation_scan_is_world_scoped',
    'generated_replay_schedule_rejects_sequential_or_wrong_order_claims',
    'generated_s3_turso_model_traces_match_every_observation',
}


def digest(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def normalized(value):
    return json.loads(json.dumps(value))


def overlap(events, schedule):
    assert schedule in SCHEDULES, 'unknown schedule'
    assert len(events) == 6, 'overlap requires six actual backend poll events'
    expected = Counter((req, event) for req in (0, 1)
                       for event in ('first_poll', 'pending', 'completed'))
    assert Counter((e['request'], e['event']) for e in events) == expected
    firsts = [e['request'] for e in events if e['event'] == 'first_poll']
    assert firsts == ([0, 1] if schedule == SCHEDULES[0] else [1, 0])
    positions = {(e['request'], e['event']): i for i, e in enumerate(events)}
    earliest_completion = min(positions[(req, 'completed')] for req in (0, 1))
    for req in (0, 1):
        assert positions[(req, 'first_poll')] < positions[(req, 'pending')] < earliest_completion


def overlap_controls():
    good = [{'request': req, 'event': event} for req, event in (
        (1, 'first_poll'), (1, 'pending'), (0, 'first_poll'), (0, 'pending'),
        (1, 'completed'), (0, 'completed'))]
    overlap(good, SCHEDULES[1])
    corruptions = [good[:3] + [good[4], good[3], good[5]],
                   good[:4] + [good[4], good[4]], good[:-1],
                   [good[i] for i in (0, 1, 4, 2, 3, 5)]]
    for corrupt in corruptions:
        try:
            overlap(corrupt, SCHEDULES[1])
        except AssertionError:
            pass
        else:
            raise AssertionError('insensitive independent overlap control')
    try:
        overlap(good, SCHEDULES[0])
    except AssertionError:
        pass
    else:
        raise AssertionError('insensitive wrong-schedule control')
    return 5


def audit(args):
    sys.path.insert(0, str(args.root / 'scripts/formal'))
    from replay_model import State, actions, observation, validate_state
    from generate_replay import validate_corpus
    report = json.loads(args.report.read_text())
    assert report['schema'] == 'fireweed-generated-replay-run-v1'
    assert report['status'] == 'PASS' and report['exit_code'] == 0
    profile = report['profile']
    assert profile in ('exhaustive', 'schedule-smoke')
    artifacts = {}
    for key in ('corpus', 'observations', 'progress', 'log'):
        actual = digest(report[key])
        assert actual == report[key + '_sha256'], (key, 'artifact hash mismatch')
        artifacts[key] = {'path': report[key], 'sha256': actual}
    before, after = report['provenance_before'], report['provenance_after']
    assert before['head'] == after['head']
    assert before['source_sha256'] == after['source_sha256']
    actual_head = subprocess.check_output(
        ['git', '-C', str(args.root), 'rev-parse', 'HEAD'], text=True).strip()
    assert actual_head == after['head'], 'current HEAD mismatch'
    for name, expected in after['source_sha256'].items():
        assert digest(args.root / name) == expected, (name, 'current source mismatch')
    assert digest(args.root / 'scripts/formal/replay_model.py') == MODEL_SHA256

    corpus = json.loads(Path(report['corpus']).read_text())
    validate_corpus(corpus)
    assert corpus['model_sha256'] == MODEL_SHA256
    assert corpus['selection']['profile'] == profile
    # Reconstruct BFS without using generator.graph or corpus transition tables.
    states, ids, graph_edges = [State()], {State(): 0}, set()
    outgoing = {}
    for source, state in enumerate(states):
        outgoing[source] = []
        for action, target, result in actions(state):
            validate_state(target)
            if target not in ids:
                ids[target] = len(states)
                states.append(target)
            edge = {'before': source, 'after': ids[target], 'result': result}
            outgoing[source].append((action, edge))
            graph_edges.add(f'{source}|{action}|{ids[target]}')
    assert len(states) == 6386 and len(graph_edges) == 20913
    assert corpus['graph'] == {'complete': True, 'states': 6386, 'edges': 20913}
    for key, exported in corpus['states'].items():
        state = states[int(key)]
        assert exported == {'model': normalized(asdict(state)), 'observation': observation(state)}
    traces = corpus['traces']
    expected_cases = 20920 if profile == 'exhaustive' else 96
    assert len(traces) == expected_cases
    total_steps = sum(len(trace['steps']) for trace in traces)
    if profile == 'exhaustive':
        assert total_steps == 153819
    detail = report['execution']
    assert detail == json.loads(Path(report['observations']).read_text()), 'embedded execution copy'
    assert detail['schema'] == 'fireweed-model-replay-evidence-v1' and detail['status'] == 'PASS'
    assert detail['corpus_sha256'] == report['corpus_sha256']
    assert detail['selection'] == corpus['selection'] and detail['model_graph'] == corpus['graph']
    assert detail['model_sha256'] == MODEL_SHA256
    assert detail['started'] == detail['attempted'] == detail['passed'] == len(detail['cases']) == len(traces)
    assert detail['workers'] == args.workers and detail['all_selected_traces_executed'] is True
    log = Path(report['log']).read_text()
    passed_tests = re.findall(r'test turso_compose::generated_replay_tests::(\w+) \.\.\. ok', log)
    assert len(passed_tests) == len(RUST_TESTS) and set(passed_tests) == RUST_TESTS
    summaries = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;.*?finished in ([\d.]+)s', log)
    assert len(summaries) == 1 and tuple(map(int, summaries[0][:3])) == (6, 0, 0)

    matched, intended, case_edges = set(), set(), {}
    action_counts, branches, reopen_counts, schedules = Counter(), Counter(), Counter(), Counter()
    forward_edges, reverse_edges, elapsed, overlap_count = set(), set(), [], 0
    for index, (case, trace) in enumerate(zip(detail['cases'], traces)):
        assert case['index'] == index and case['trace'] == trace['id'] and case['status'] == 'PASS'
        assert case['error'] is None and case['intended_edges'] == trace['intended_edges']
        assert case['base_trace'] == trace['base_trace'] and case['schedule'] == trace['schedule']
        schedule = trace['schedule']
        assert schedule in SCHEDULES
        schedules[schedule] += 1
        sequence = [step['action'] for step in trace['steps']]
        base = hashlib.sha256(json.dumps(sequence, separators=(',', ':')).encode()).hexdigest()[:16]
        assert base == trace['base_trace']
        expected_id = base if schedule == SCHEDULES[0] else hashlib.sha256((base + '|' + schedule).encode()).hexdigest()[:16]
        assert trace['id'] == expected_id
        assert len(case['steps']) == len(trace['steps'])
        current, witnessed = {0}, set()
        elapsed.append(case['elapsed_ms'])
        assert 0 <= case['elapsed_ms'] <= 20000, ('trace elapsed outside 20-second bound', index)
        for step_index, (observed, predicted) in enumerate(zip(case['steps'], trace['steps'])):
            where = (index, step_index, predicted['action'])
            action = predicted['action']
            assert observed['step'] == step_index and observed['action'] == action and observed['status'] == 'PASS', where
            assert set(observed['compatible_before']) == current and len(observed['compatible_before']) == len(current), (where, 'predecessor set')
            following, edges = set(), set()
            for source in current:
                for label, edge in outgoing[source]:
                    if label == action and edge['result'] == observed['result'] and observation(states[edge['after']]) == observed['observation']:
                        following.add(edge['after'])
                        edges.add(f'{source}|{action}|{edge["after"]}')
            assert following, (where, 'no compatible independent model path')
            assert following == set(observed['compatible_after']) and len(following) == len(observed['compatible_after']), (where, 'successor set')
            assert edges == set(observed['matched_edges']) and len(edges) == len(observed['matched_edges']), (where, 'matched edges')
            assert len(following) == 1, (where, 'ambiguous witnessed state')
            model = states[next(iter(following))]
            diagnostics = observed['diagnostics']
            physical = [item for item in diagnostics if item['operation'] == 'Observe']
            assert len(physical) == 1, (where, 'physical observation multiplicity')
            physical = physical[0]
            positions = physical['durable_positions']
            assert len(positions) == len(model.history) + 1, (where, 'durable history length')
            assert [p['sequence'] for p in positions] == list(range(len(positions))), (where, 'durable sequence')
            assert all(p['queue'] == {'tenant_id': 'formal', 'queue_id': 'claims'} for p in positions)
            assert positions[0]['backend_epoch'] == 0
            assert [p['backend_epoch'] for p in positions[1:]] == [record[2] for record in model.history]
            assert physical['projection_before'] == physical['projection_after'] == positions[model.applied], (where, 'exact side-effect-free applied frontier')
            reopen = [item for item in diagnostics if item['operation'] == 'Reopen']
            if action == 'Reopen':
                assert len(reopen) == 1
                reopen = reopen[0]
                prefixes = {states[state].applied for state in current}
                assert len(prefixes) == 1
                assert reopen['pre_close'] == reopen['post_close'] == positions[next(iter(prefixes))]
                assert reopen['post_open'] == positions[-1]
                assert reopen['tail_repaired'] == (reopen['pre_close'] != reopen['post_open'])
                assert reopen['same_namespace'] is True and reopen['same_projection_file'] is True
                reopen_counts['tail_repaired' if reopen['tail_repaired'] else 'retained_state'] += 1
            else:
                assert not reopen
            concurrent = [item for item in diagnostics if item['operation'] == 'ConcurrentSchedule']
            if action == 'ConcurrentClaims':
                assert len(concurrent) == 1 and concurrent[0]['schedule'] == schedule
                overlap(concurrent[0]['events'], schedule)
                overlap_count += 1
                responses = observed['result']['responses']
                assert [r['request'] for r in responses] == [0, 1]
                items = [r['item'] for r in responses]
                assert items == ([0, 1] if schedule == SCHEDULES[0] else [1, 0]), (where, 'schedule did not realize intended stimulus')
                assert [record[0] for record in model.history] == ([0, 1] if schedule == SCHEDULES[0] else [1, 0])
                branches[str(items)] += 1
            else:
                assert not concurrent
            assert {item['operation'] for item in diagnostics} <= {'Observe', 'Reopen', 'ConcurrentSchedule'}
            action_counts[action] += 1
            current = following
            witnessed.update(edges)
        intended.update(trace['intended_edges'])
        matched.update(witnessed)
        (forward_edges if schedule == SCHEDULES[0] else reverse_edges).update(witnessed)
        case_edges[index] = witnessed
    assert matched == set(detail['matched_edges']) and len(matched) == len(detail['matched_edges'])
    assert matched <= graph_edges and sum(action_counts.values()) == total_steps
    exhaustive = matched == graph_edges
    assert detail['sut_graph_edge_exhaustive'] == exhaustive
    assert report['coverage'] == {'intended': len(intended), 'matched': len(matched),
        'intended_unwitnessed': len(intended - matched), 'graph_edge_exhaustive': exhaustive}
    if profile == 'exhaustive':
        assert intended == matched == graph_edges
        assert schedules == {SCHEDULES[0]: 17002, SCHEDULES[1]: 3918}
        assert len(forward_edges) == 16995 and len(reverse_edges - forward_edges) == 3918
        assert branches == {'[0, 1]': 3919, '[1, 0]': 3918}
    events = [json.loads(line) for line in Path(report['progress']).read_text().splitlines()]
    assert len(events) == len(traces) and {e['index'] for e in events} == set(range(len(traces)))
    accumulated = set()
    for completion, event in enumerate(events, 1):
        case = detail['cases'][event['index']]
        accumulated.update(case_edges[event['index']])
        assert event['schema'] == 'fireweed-replay-progress-v1' and event['trace'] == case['trace']
        assert event['base_trace'] == case['base_trace'] and event['schedule'] == case['schedule']
        assert event['status'] == 'PASS' and event['total'] == len(traces)
        assert event['completed'] == event['passed'] == completion
        assert event['elapsed_ms'] == case['elapsed_ms'] and event['matched_edges'] == len(accumulated)
    assert accumulated == matched

    fixture = report['s3_fixture']
    cas_path = Path(fixture['native_cas_evidence'])
    cas = json.loads(cas_path.read_text())
    assert cas['status'] == 'passed' and cas['endpoint'] == fixture['endpoint']
    assert cas['native_atomic_conditional_create'] is True and cas['native_atomic_conditional_update'] is True
    assert cas['sequential_create_only']['first_status'] == 200 and cas['sequential_create_only']['second_status'] == 412
    assert cas['sequential_create_only']['winner_body_preserved'] is True
    assert sorted(item['status'] for item in cas['two_writer_create_only_race']['writer_results']) == [200, 412]
    assert cas['conditional_update']['if_match_current_status'] == 200 and cas['conditional_update']['if_match_stale_status'] == 412
    assert fixture['console_enabled'] is False and fixture['endpoint'].startswith('http://127.0.0.1:')
    assert digest(args.rustfs_binary) == fixture['binary_sha256'], 'RustFS binary identity'
    parent, private = Path(fixture['fixture_parent']), Path(fixture['private_root'])
    assert parent.is_absolute() and parent.resolve() == parent and parent.is_dir()
    assert private.is_absolute() and private.parent == parent
    assert private.name.startswith('fireweed-formal-s3-') and private.name != 'fireweed-formal-s3-'
    assert Path(fixture['data_root']) == private / 's3'
    assert Path(fixture['worlds_tmpdir']) == private / 'worlds'
    assert fixture['cleanup_confirmed'] is True and not private.exists(), 'fixture root remains'
    assert fixture['capacity_budget'] == {
        'expected_cases': len(traces), 'inodes': 64 * len(traces) + 8192,
        'bytes': 65536 * len(traces) + 128 * 1024 * 1024}
    filesystem = [fixture[name] for name in (
        'filesystem_before', 'filesystem_after_execution', 'filesystem_after_cleanup')]
    assert {snapshot['device'] for snapshot in filesystem} == {os.stat(parent).st_dev}
    for snapshot in filesystem:
        assert all(isinstance(snapshot[key], int) and snapshot[key] >= 0
                   for key in ('device', 'inode_total', 'inode_free', 'inode_available', 'bytes_available'))
        if snapshot['inode_total']:
            assert snapshot['inode_available'] <= snapshot['inode_free'] <= snapshot['inode_total']
    if filesystem[0]['inode_total']:
        assert filesystem[0]['inode_available'] >= fixture['capacity_budget']['inodes']
    assert filesystem[0]['bytes_available'] >= fixture['capacity_budget']['bytes']
    # These are filesystem-wide net changes, not per-fixture allocation counts.
    fixture_usage = {
        'parent': str(parent), 'device': filesystem[0]['device'],
        'capacity_budget': fixture['capacity_budget'], 'cleanup_confirmed': True,
        'filesystem_before': filesystem[0], 'filesystem_after_execution': filesystem[1],
        'filesystem_after_cleanup': filesystem[2],
        'net_inodes_consumed_during_execution': filesystem[0]['inode_available'] - filesystem[1]['inode_available'],
        'net_inodes_recovered_during_cleanup': filesystem[2]['inode_available'] - filesystem[1]['inode_available'],
    }
    workflow = args.root / '.github/workflows/ci.yml'
    workflow_text = workflow.read_text()
    assert '--replay-profile schedule-smoke --workers 8' in workflow_text
    assert '--replay-profile smoke ' not in workflow_text
    artifacts['ci_workflow_wiring'] = {'path': str(workflow), 'sha256': digest(workflow),
                                     'execution_claim': 'source wiring reviewed; remote CI was not executed by this audit'}
    artifacts['native_cas'] = {'path': str(cas_path), 'sha256': digest(cas_path)}
    artifacts['report'] = {'path': str(args.report), 'sha256': digest(args.report)}
    return {
        'schema': 'fireweed-independent-scheduled-audit-v1', 'status': 'PASS',
        'disposition': 'APPROVE within declared bounded backend replay scope',
        'profile': profile, 'source_head': actual_head,
        'current_source_files_verified': len(after['source_sha256']), 'unchanged_model_sha256': MODEL_SHA256,
        'canonical_graph': corpus['graph'], 'cases': len(traces), 'steps': total_steps,
        'coverage': report['coverage'], 'workers': detail['workers'],
        'schedule_case_counts': dict(schedules), 'forward_edges': len(forward_edges),
        'reverse_additional_edges': len(reverse_edges - forward_edges),
        'backend_overlap_witnesses': overlap_count, 'independent_overlap_negative_controls': overlap_controls(),
        'exact_rust_tests_verified': sorted(RUST_TESTS), 'rust_test_elapsed_seconds': float(summaries[0][3]),
        'case_elapsed_ms': {'min': min(elapsed), 'median': statistics.median(elapsed), 'max': max(elapsed)},
        'action_counts': dict(action_counts), 'concurrent_claim_item_orders': dict(branches),
        'reopen_counts': dict(reopen_counts), 'progress_events_reconciled': len(events),
        'rustfs_binary_sha256': fixture['binary_sha256'], 'rustfs_binary_verified': True,
        'fixture_storage': fixture_usage,
        'artifacts': artifacts, 'audit_script_sha256': digest(__file__),
        'limits': ['one-process ephemeral control plane', 'real local RustFS and Turso, not production-cloud S3',
            'backend-level composition, not facade routing or API-007 queue pipeline',
            'graceful reopen, not process-kill recovery', 'simulated client nonreceipt',
            'scheduled bounded graph edge coverage, not universal refinement or numerical production deadlines'],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--rustfs-binary', type=Path, required=True)
    parser.add_argument('--workers', type=int, default=8)
    args = parser.parse_args()
    result = audit(args)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
