#!/usr/bin/env python3
"""Execute the exact API-007 PostgreSQL debt routes; no performance claim.

A report remains INCOMPLETE until every route and owned-fixture cleanup succeeds.
The legacy S3 control uses a fresh checksum-pinned RustFS qualification endpoint.
"""
import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import signal
import shutil
import tempfile
import subprocess
import time

from queue_owned_process import CANCELLATION_SIGNALS, drain_adopted, own_descendants, owned_process, run_owned
from queue_pg_evidence import digest, sources
from queue_pg_fixture import QueuePostgresFixture
from queue_pg_routes import cargo_route, load_routes, validate_execution, validate_listing

ROOT = Path(__file__).resolve().parents[2]


def run(command, log, environment, timeout):
    return run_owned(command, log, environment, timeout, ROOT)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pg-bin', required=True, type=Path)
    parser.add_argument('--fixture-parent', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--timeout', type=int, default=900)
    args = parser.parse_args()
    output = args.output.absolute()
    if output.is_symlink() or not output.resolve().is_relative_to((ROOT / 'target').resolve()):
        raise ValueError('evidence output must be a new directory beneath checkout target')
    output.mkdir(parents=True, exist_ok=False)
    report = {'schema': 'fireweed.queue_pg_execution.v1', 'status': 'INCOMPLETE',
              'scope': 'correctness_only_not_tp005', 'routes': [], 'child_listings': [], 'cleanup': False}
    report_path = output / 'report.json'

    def save():
        temporary = output / 'report.pending'
        temporary.write_text(json.dumps(report, indent=2) + '\n')
        temporary.replace(report_path)

    save()
    routes = load_routes(ROOT)
    report['revision'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    report['sources'] = sources()
    save()
    fixture = QueuePostgresFixture(args.pg_bin, args.fixture_parent)
    secret_root = Path(tempfile.mkdtemp(prefix='fireweed-pg-s3-', dir=args.fixture_parent)).resolve()
    if secret_root.is_relative_to(ROOT):
        secret_root.rmdir()
        raise ValueError('fixture parent must be outside checkout')
    s3_environment = dict(os.environ, FIREWEED_S3_SECRET_DIR=str(secret_root))
    helper = ['bash', 'scripts/ci/s3-qualification-endpoint.sh']
    provision_owner = ExitStack()
    provision = None
    try:
        provision_log = provision_owner.enter_context((output / 's3-provision.log').open('x'))
        provision = provision_owner.enter_context(owned_process(
            helper + ['provision'], cwd=ROOT, env=s3_environment,
            stdout=provision_log, stderr=subprocess.STDOUT))
        if provision.wait(timeout=args.timeout):
            raise RuntimeError('S3 qualification provisioning failed; see s3-provision.log')
        run(helper + ['verify-isolation'], output / 's3-isolation.log', s3_environment, args.timeout)
        for line in (secret_root / 'credentials.env').read_text().splitlines():
            if line and not line.startswith('#'):
                key, value = line.split('=', 1)
                if not key.startswith('FIREWEED_S3_TEST_'):
                    raise ValueError('unexpected qualification credential key')
                s3_environment[key] = value
        report['s3_attestation'] = json.loads((secret_root / 's3-native-cas-capability-attestation.json').read_text())
        fixture.start()
        report['postgres_version'] = fixture.version
        report['postgres_qualification'] = fixture.qualification
        environment = fixture.test_environment()
        # Exactly one test is selected per serial route. Use libtest's concurrent
        # formatter so inherited child/stdout cannot split its successful name.
        environment['RUST_TEST_THREADS'] = '2'
        report['libtest_threads'] = 2
        environment['FIREWEED_API007_BENCH_RECORDS'] = '100'
        for suffix in ('ENDPOINT', 'BUCKET', 'ACCESS_KEY', 'SECRET_KEY'):
            value = s3_environment.get('FIREWEED_S3_TEST_' + suffix)
            if not value:
                raise ValueError(f'missing qualification fixture variable FIREWEED_S3_TEST_{suffix}')
            environment['FIREWEED_TEST_S3_' + suffix] = value
        for entry in routes:
            command = cargo_route(entry)
            listing = output / (entry['debt_id'] + '.list.log')
            listing_command = command + ['--list', '--format', 'terse']
            validate_listing(entry, run(listing_command, listing, environment, args.timeout))
            if entry['kind'] == 'child':
                report['child_listings'].append({'debt_id': entry['debt_id'],
                    'command': listing_command, 'listing': listing.name,
                    'listing_sha256': digest(listing)})
                save()
                continue
            log = output / (entry['debt_id'] + '.run.log')
            start = time.monotonic()
            validate_execution(entry, run(command + ['--nocapture'], log, environment, args.timeout))
            covered = [entry['debt_id']] + [r['debt_id'] for r in routes
                if r.get('parent_debt_id') == entry['debt_id']]
            report['routes'].append({'debt_ids': covered, 'command': command,
                'listing': listing.name, 'listing_sha256': digest(listing),
                'listing_command': listing_command, 'execution_command': command + ['--nocapture'],
                'log': log.name, 'log_sha256': digest(log),
                'seconds': time.monotonic() - start})
            save()
    finally:
        previous_signals = signal.pthread_sigmask(signal.SIG_BLOCK, CANCELLATION_SIGNALS)
        cleanup_errors = []
        # Interrupted provisioning may still create state/start a daemon. Quiesce
        # that producer before inspecting its state or attempting teardown.
        if provision is None or provision.poll() is None:
            try:
                provision_owner.close()
            except Exception as error:
                cleanup_errors.append(str(error))
        try:
            fixture.close()
        except Exception as error:
            cleanup_errors.append(str(error))
        try:
            run(helper + ['teardown'], output / 's3-teardown.log', s3_environment, 60)
            attestation = secret_root / 's3-native-cas-capability-attestation.json'
            if attestation.exists():
                report['s3_attestation'] = json.loads(attestation.read_text())
        except Exception as error:
            cleanup_errors.append(str(error))
        try:
            provision_owner.close()
        except Exception as error:
            cleanup_errors.append(str(error))
        try:
            report['reaped_adopted'] = drain_adopted()
        except Exception as error:
            cleanup_errors.append(str(error))
        if not cleanup_errors:
            try:
                shutil.rmtree(secret_root)
            except Exception as error:
                cleanup_errors.append(str(error))
        report['cleanup'] = fixture.cleaned and not cleanup_errors
        report['cleanup_errors'] = cleanup_errors
        save()
        signal.pthread_sigmask(signal.SIG_SETMASK, previous_signals)
        if cleanup_errors:
            raise RuntimeError('fixture cleanup failed: ' + '; '.join(cleanup_errors))
    if subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip() != report['revision']:
        raise RuntimeError('HEAD changed during execution')
    if sources() != report['sources']:
        raise RuntimeError('source files changed during execution')
    covered = [i for r in report['routes'] for i in r['debt_ids']]
    if len(covered) != len(set(covered)) or set(covered) != {r['debt_id'] for r in routes}:
        raise RuntimeError('incomplete or duplicate debt execution coverage')
    report['fixture_logs'] = {name: digest(output / name) for name in
        ('s3-provision.log', 's3-isolation.log', 's3-teardown.log')}
    report['status'] = 'PASS'
    save()


if __name__ == '__main__':
    own_descendants()
    main()
