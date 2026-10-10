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
from generate_replay import generate, validate_corpus


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--profile', choices=('smoke', 'edges'), default='smoke')
    parser.add_argument('--offline', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    corpus_path = args.output.with_name(args.output.stem + '-corpus.json').resolve()
    detail_path = args.output.with_name(args.output.stem + '-observations.json').resolve()
    log_path = args.output.with_suffix('.log').resolve()
    report = {'schema': 'fireweed-generated-replay-run-v1', 'status': 'FAIL',
              'profile': args.profile, 'provenance_before': provenance(root)}
    try:
        corpus = generate(args.profile)
        validate_corpus(corpus)
        corpus_path.write_text(json.dumps(corpus, sort_keys=True, separators=(',', ':')) + '\n')
        # Remove only this run's stale output: absent/new evidence must fail closed.
        detail_path.unlink(missing_ok=True)
        command = ['cargo', 'test', '--locked', '-p', 'fireweed', '--lib', 'generated_replay']
        if args.offline:
            command.append('--offline')
        command += ['--', '--test-threads=1']
        env = dict(os.environ, FIREWEED_REPLAY_CORPUS=str(corpus_path), FIREWEED_REPLAY_EVIDENCE=str(detail_path))
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
        if process.returncode or detail['status'] != 'PASS' or not detail['all_selected_traces_executed']:
            raise ValueError('production trace replay failed; failing prefix retained')
        if detail['corpus_sha256'] != report['corpus_sha256'] or detail['passed'] != len(corpus['traces']) or detail['attempted'] != len(corpus['traces']):
            raise ValueError('executed corpus identity/count mismatch')
        summaries = re.findall(r'test result: ok\. (\d+) passed; 0 failed; (\d+) ignored', log_path.read_text())
        if not summaries or sum(int(row[0]) for row in summaries) < 4 or any(int(row[1]) for row in summaries):
            raise ValueError('decoder/oracle controls missing, ignored, or failed')
        after = provenance(root)
        report['provenance_after'] = after
        if after['head'] != report['provenance_before']['head'] or after['source_sha256'] != report['provenance_before']['source_sha256']:
            raise ValueError('source changed during replay run')
        report['status'] = 'PASS'
    except (ValueError, KeyError, TypeError, AssertionError, OSError) as error:
        report['error'] = str(error)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
    print(json.dumps({'status': report['status'], 'error': report.get('error'),
                      'traces': report.get('execution', {}).get('passed', 0)}, sort_keys=True))
    return 0 if report['status'] == 'PASS' else 1


if __name__ == '__main__':
    sys.exit(main())
