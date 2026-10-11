#!/usr/bin/env python3
"""Generate, validate and execute a source-bound real S3/Turso replay corpus."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from check import provenance
from generate_replay import generate, validate_corpus, graph, scheduled_edges


def reconcile_witnessed(corpus, detail):
    """Rebuild witnessed coverage from per-step evidence; never trust totals."""
    witnessed = set()
    for trace, case in zip(corpus['traces'], detail['cases']):
        case_edges = set()
        for step in case['steps']:
            edges = step['matched_edges']
            if not edges or len(edges) != len(set(edges)):
                raise ValueError('empty or duplicate per-step edge witnesses')
            case_edges.update(edges)
        if trace.get('schedule') and case_edges != scheduled_edges(trace, trace['schedule']):
            raise ValueError('requested schedule not realized in observed model path')
        witnessed.update(case_edges)
    if len(witnessed) != len(detail['matched_edges']) or witnessed != set(detail['matched_edges']):
        raise ValueError('aggregate coverage differs from actual per-step witnesses')
    graph_edges = {f'{a}|{b}|{c}' for a, b, c, _ in graph()[3]}
    if not witnessed <= graph_edges:
        raise ValueError('observed edge outside canonical graph')
    exhaustive = witnessed == graph_edges
    if detail['sut_graph_edge_exhaustive'] != exhaustive:
        raise ValueError('forged graph exhaustiveness claim')
    if corpus['selection']['profile'] == 'exhaustive' and not exhaustive:
        raise ValueError('scheduled replay did not witness every canonical graph edge')
    return witnessed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--profile', choices=('smoke', 'edges', 'exhaustive', 'schedule-smoke'), default='smoke')
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--workers', type=int, choices=range(1,17), default=1)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    corpus_path = args.output.with_name(args.output.stem + '-corpus.json').resolve()
    detail_path = args.output.with_name(args.output.stem + '-observations.json').resolve()
    log_path = args.output.with_suffix('.log').resolve()
    progress_path = args.output.with_name(args.output.stem + '-progress.jsonl').resolve()
    report = {'schema': 'fireweed-generated-replay-run-v1', 'status': 'FAIL',
              'profile': args.profile, 'provenance_before': provenance(root)}
    try:
        corpus = generate(args.profile)
        validate_corpus(corpus)
        corpus_path.write_text(json.dumps(corpus, sort_keys=True, separators=(',', ':')) + '\n')
        # Remove only this run's stale output: absent/new evidence must fail closed.
        detail_path.unlink(missing_ok=True)
        progress_path.unlink(missing_ok=True)
        command = ['cargo', 'test', '--locked', '-p', 'fireweed', '--lib', 'generated_replay']
        if args.offline:
            command.append('--offline')
        command += ['--', '--test-threads=1']
        env = dict(os.environ, FIREWEED_REPLAY_CORPUS=str(corpus_path), FIREWEED_REPLAY_EVIDENCE=str(detail_path),
                   FIREWEED_REPLAY_PROGRESS=str(progress_path), FIREWEED_REPLAY_WORKERS=str(args.workers))
        print(f'Generated {len(corpus["traces"])} traces from {corpus["graph"]["states"]} states / {corpus["graph"]["edges"]} edges', flush=True)
        with log_path.open('w') as log:
            process = subprocess.run(command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
        report.update(command=command, exit_code=process.returncode,
            corpus=str(corpus_path), corpus_sha256=hashlib.sha256(corpus_path.read_bytes()).hexdigest(),
            log=str(log_path), log_sha256=hashlib.sha256(log_path.read_bytes()).hexdigest())
        if not detail_path.exists():
            raise ValueError('Rust replay produced no fresh observation evidence')
        detail = json.loads(detail_path.read_text())
        report['observations'] = str(detail_path)
        report['observations_sha256'] = hashlib.sha256(detail_path.read_bytes()).hexdigest()
        report['execution'] = detail
        events = [json.loads(line) for line in progress_path.read_text().splitlines()]
        report['progress'] = str(progress_path)
        report['progress_sha256'] = hashlib.sha256(progress_path.read_bytes()).hexdigest()
        if len(events) != detail['attempted'] or len({event['index'] for event in events}) != len(events):
            raise ValueError('progress completion identity/count mismatch')
        if process.returncode or detail['status'] != 'PASS' or not detail['all_selected_traces_executed']:
            raise ValueError('production trace replay failed; failing prefix retained')
        if detail['corpus_sha256'] != report['corpus_sha256'] or detail['passed'] != len(corpus['traces']) or detail['attempted'] != len(corpus['traces']):
            raise ValueError('executed corpus identity/count mismatch')
        summaries = re.findall(r'test result: ok\. (\d+) passed; 0 failed; (\d+) ignored', log_path.read_text())
        if not summaries or sum(int(row[0]) for row in summaries) < 6 or any(int(row[1]) for row in summaries):
            raise ValueError('decoder/oracle controls missing, ignored, or failed')
        if {event['index'] for event in events} != set(range(len(corpus['traces']))) or any(
                event['status'] != 'PASS' or event['trace'] != corpus['traces'][event['index']]['id']
                or event.get('schedule') != corpus['traces'][event['index']].get('schedule')
                or event.get('base_trace') != corpus['traces'][event['index']].get('base_trace') for event in events):
            raise ValueError('incomplete or mismatched progress completions')
        if len(detail['cases']) != len(corpus['traces']) or any(
                case['index'] != index or case['trace'] != trace['id'] or case['status'] != 'PASS'
                or case.get('schedule') != trace.get('schedule') or case.get('base_trace') != trace.get('base_trace')
                or len(case['steps']) != len(trace['steps'])
                or any(observed['action'] != predicted['action'] or observed['status'] != 'PASS'
                       for observed, predicted in zip(case['steps'], trace['steps']))
                for index, (case, trace) in enumerate(zip(detail['cases'], corpus['traces']))):
            raise ValueError('per-case action evidence incomplete or mismatched')
        intended = {edge for trace in corpus['traces'] for edge in trace['intended_edges']}
        matched = reconcile_witnessed(corpus, detail)
        report['coverage'] = {'intended':len(intended), 'matched':len(matched),
            'intended_unwitnessed':len(intended - matched),
            'graph_edge_exhaustive':detail['sut_graph_edge_exhaustive']}
        after = provenance(root)
        report['provenance_after'] = after
        if after['head'] != report['provenance_before']['head'] or after['source_sha256'] != report['provenance_before']['source_sha256']:
            raise ValueError('source changed during replay run')
        report['status'] = 'PASS'
    except (ValueError, KeyError, TypeError, AssertionError, OSError) as error:
        report['error'] = str(error)
    with args.output.open('w') as output:
        json.dump(report, output, sort_keys=True, separators=(',', ':'))
        output.write('\n')
    print(json.dumps({'status': report['status'], 'error': report.get('error'),
                      'traces': report.get('execution', {}).get('passed', 0)}, sort_keys=True))
    return 0 if report['status'] == 'PASS' else 1


if __name__ == '__main__':
    sys.exit(main())
