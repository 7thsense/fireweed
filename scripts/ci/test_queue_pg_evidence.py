"""Negative controls for accepting execution evidence at the closure boundary."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import queue_pg_evidence as evidence
from queue_pg_routes import cargo_route, load_routes


class EvidenceControls(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='queue-evidence-control-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.output = self.root / 'target' / 'evidence'
        self.output.mkdir(parents=True)
        self.path = self.output / 'report.json'
        self.routes = load_routes(evidence.ROOT)
        self.report = {
            'schema': 'fireweed.queue_pg_execution.v1', 'scope': 'correctness_only_not_tp005',
            'status': 'PASS', 'cleanup': True, 'cleanup_errors': [], 'libtest_threads': 2,
            'revision': 'test-revision', 'sources': {'Cargo.lock': 'test-lock-digest'},
            'postgres_version': 'postgres (PostgreSQL) 16.15', 'reaped_adopted': [],
            'postgres_qualification': {'current_user': 'fireweed_queue',
                'data_directory_verified': True, 'server_version_num': 160015,
                'privileges': dict.fromkeys(('superuser', 'createdb', 'createrole', 'replication', 'bypassrls'), False),
                'durability': dict.fromkeys(('fsync', 'synchronous_commit', 'full_page_writes'), 'on')},
            'routes': [], 'child_listings': [], 'fixture_logs': {},
            's3_attestation': {'results': {'selected': True}, 's3': {
                'provider': 'rustfs', 'version': '1.0.0',
                'artifact_sha256': 'c30a95b76546f25122c9ca387090ddb30c391ca5605621b0d7c881703c0f21c8'},
                'preflight': {'status': 'passed'}, 'cleanup': {'status': 'completed'}},
        }
        for entry in self.routes:
            command = cargo_route(entry)
            listing = self.output / (entry['debt_id'] + '.list.log')
            listing.write_text(entry['test'] + ': test\n')
            if entry['kind'] == 'child':
                self.report['child_listings'].append({'debt_id': entry['debt_id'],
                    'command': command + ['--list', '--format', 'terse'],
                    'listing': listing.name, 'listing_sha256': evidence.digest(listing)})
                continue
            log = self.output / (entry['debt_id'] + '.run.log')
            names = (['process_loss_child', entry['test']] if entry['kind'] == 'process_parent' else [entry['test']])
            log.write_text(''.join('test ' + name + ' ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n' for name in names))
            self.report['routes'].append({'debt_ids': [entry['debt_id']] + [
                r['debt_id'] for r in self.routes if r.get('parent_debt_id') == entry['debt_id']],
                'command': command, 'listing_command': command + ['--list', '--format', 'terse'],
                'execution_command': command + ['--nocapture'], 'listing': listing.name,
                'listing_sha256': evidence.digest(listing), 'log': log.name,
                'log_sha256': evidence.digest(log)})
        for name in ('s3-provision.log', 's3-isolation.log', 's3-teardown.log'):
            (self.output / name).write_text('synthetic control log\n')
            self.report['fixture_logs'][name] = evidence.digest(self.output / name)
        for mock in (patch.object(evidence, 'ROOT', self.root),
                     patch.object(evidence, 'sources', return_value=self.report['sources']),
                     patch.object(evidence, 'load_routes', return_value=self.routes),
                     patch.object(evidence.subprocess, 'check_output', return_value='test-revision')):
            mock.start()
            self.addCleanup(mock.stop)

    def verify(self, report=None):
        self.path.write_text(json.dumps(self.report if report is None else report))
        return evidence.verify_report(self.path)

    def test_positive_exact_coverage(self):
        self.assertEqual(self.verify(), {r['debt_id'] for r in self.routes})

    def test_structural_and_stale_evidence_fails(self):
        mutations = [
            lambda r: r.update(status='INCOMPLETE'),
            lambda r: r.pop('libtest_threads'),
            lambda r: r.update(libtest_threads=1),
            lambda r: r.update(cleanup=False),
            lambda r: r.update(cleanup_errors=['stop failed']),
            lambda r: r.update(revision='old-revision'),
            lambda r: r.update(sources={'Cargo.lock': 'old-lock'}),
            lambda r: r.update(postgres_version='postgres (PostgreSQL) 17.0'),
            lambda r: r['postgres_qualification']['privileges'].update(superuser=True),
            lambda r: r['postgres_qualification']['durability'].update(fsync='off'),
            lambda r: r['routes'].pop(),
            lambda r: r['routes'].append(copy.deepcopy(r['routes'][0])),
            lambda r: r['routes'][0]['debt_ids'].append('unknown-debt'),
            lambda r: r['routes'][0]['execution_command'].append('--skip'),
            lambda r: r['routes'][0].update(log='../outside.log'),
            lambda r: r['routes'][0].update(log_sha256='changed'),
            lambda r: r['routes'][0].update(log=r['routes'][1]['log'], log_sha256=r['routes'][1]['log_sha256']),
            lambda r: r['child_listings'].clear(),
            lambda r: r['fixture_logs'].pop('s3-teardown.log'),
            lambda r: r['s3_attestation']['cleanup'].update(status='pending'),
        ]
        for mutate in mutations:
            candidate = copy.deepcopy(self.report)
            mutate(candidate)
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                self.verify(candidate)

    def test_zero_ignored_extra_and_parent_missing_summary_fail(self):
        ordinary = self.report['routes'][0]
        parent = next(r for r in self.report['routes'] if len(r['debt_ids']) == 2)
        summary = 'test result: ok. 1 passed; 0 failed; 0 ignored;\n'
        ordinary_name = 'test ' + ordinary['command'][-1] + ' ... ok\n'
        parent_names = 'test process_loss_child ... ok\ntest ' + parent['command'][-1] + ' ... ok\n'
        for route, text in [
            (ordinary, ordinary_name + summary.replace('1 passed', '0 passed')),
            (ordinary, ordinary_name + summary.replace('0 ignored', '1 ignored')),
            (ordinary, ordinary_name + summary * 2),
            (parent, parent_names + summary),
        ]:
            log = self.output / route['log']
            previous = log.read_text()
            log.write_text(text)
            route['log_sha256'] = evidence.digest(log)
            with self.assertRaises(ValueError):
                self.verify()
            log.write_text(previous)
            route['log_sha256'] = evidence.digest(log)

    def test_other_test_content_under_expected_filename_fails(self):
        route, other = self.report['routes'][:2]
        expected_log = self.output / route['log']
        expected_log.write_bytes((self.output / other['log']).read_bytes())
        route['log_sha256'] = evidence.digest(expected_log)
        with self.assertRaises(ValueError):
            self.verify()

    def test_symlinked_raw_log_fails(self):
        route = self.report['routes'][0]
        log = self.output / route['log']
        target = self.output / 'actual.log'
        log.rename(target)
        log.symlink_to(target)
        with self.assertRaises(ValueError):
            self.verify()


if __name__ == '__main__':
    unittest.main()
