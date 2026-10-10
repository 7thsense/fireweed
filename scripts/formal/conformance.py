#!/usr/bin/env python3
"""Run source-specific Rust seam/Turso/public-S3 evidence independently of models."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
from check import provenance

SUITES = [
    ('engine-seams', 'generic', ['-p', 'fireweed-engine', '--lib']),
    ('turso-lifecycle-mutation', 'turso', ['-p', 'fireweed-turso', '--features', 'local',
        '--test', 'async_reclaim_selection', '--test', 'item_mutation_parity']),
    ('cross-boundary-witnesses', 'filesystem-log-turso', ['-p', 'fireweed', '--lib', 'formal_analysis_tests']),
    ('packed-apply', 'generic-memory-projection', ['-p', 'fireweed-objectlog', '--lib', 'async_projection_apply::tests']),
    ('publication-recovery', 'mixed-filesystem-log-and-s3-cas', ['-p', 'fireweed-objectlog', '--lib',
                                             'log_engine_store::tests']),
    ('async-debt-ambiguity', 'filesystem-log-memory-projection', ['-p', 'fireweed-objectlog', '--lib',
                                                               'async_product::tests::async_projection']),
    ('public-receipt-recovery', 'public-s3-turso', ['-p', 'fireweed', '--test', 'request_id_idempotency']),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--suite', choices=[s[0] for s in SUITES], action='append')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    report = {'schema': 'fireweed-formal-conformance-v1', 'status': 'PASS',
              'provenance_before': provenance(root), 'suites': [],
              'exclusions': ['whole-program refinement', 'universal FR-12 deadline proof',
                             'OS process-kill qualification', 'production-cloud S3 qualification']}
    log_root = args.output.parent / (args.output.stem + '-logs')
    log_root.mkdir(parents=True, exist_ok=True)
    for name, attribution, test_args in SUITES:
        if args.suite and name not in args.suite:
            continue
        command = ['cargo', 'test', '--locked', *test_args]
        if args.offline:
            command.append('--offline')
        command += ['--', '--test-threads=1']
        print(f'Running {name} ({attribution})', flush=True)
        # Cargo/test diagnostics are retained in a log, not mixed into JSON.
        log = log_root / f'{name}.log'
        with log.open('w') as output:
            process = subprocess.run(command, cwd=root, stdout=output, stderr=subprocess.STDOUT)
        content = log.read_text()
        summaries = re.findall(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored', content)
        passed = sum(int(row[1]) for row in summaries)
        ignored = sum(int(row[3]) for row in summaries)
        status = 'PASS' if process.returncode == 0 and passed > 0 and ignored == 0 else 'FAIL'
        report['suites'].append({'name': name, 'attribution': attribution, 'command': command,
            'exit_code': process.returncode, 'status': status, 'passed': passed,
            'ignored': ignored, 'log': str(log.resolve()),
            'log_sha256': hashlib.sha256(log.read_bytes()).hexdigest()})
        print(f'{name}: {status}, {passed} passed, {ignored} ignored', flush=True)
        if status != 'PASS':
            report['status'] = 'FAIL'
    after = provenance(root)
    report['provenance_after'] = after
    if after['head'] != report['provenance_before']['head'] or after['source_sha256'] != report['provenance_before']['source_sha256']:
        report.update(status='FAIL', error='source changed during conformance run')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
    return 0 if report['status'] == 'PASS' else 1


if __name__ == '__main__':
    sys.exit(main())
