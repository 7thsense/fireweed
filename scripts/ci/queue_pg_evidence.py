"""Current-source and exact-log verification for PostgreSQL execution evidence."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import stat
from queue_pg_routes import cargo_route, load_routes, validate_execution, validate_listing

ROOT = Path(__file__).resolve().parents[2]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sources():
    listing = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=ROOT)
    result = {}
    for raw in set(listing.split(b'\0')) - {b''}:
        path = ROOT / os.fsdecode(raw)
        if path.parts[len(ROOT.parts)] == 'target':
            continue
        if path.is_symlink():
            raise ValueError(f'source symlink unsupported: {path}')
        if path.is_file():
            result[str(path.relative_to(ROOT))] = digest(path)
    return dict(sorted(result.items()))



def require(condition, message):
    if not condition:
        raise ValueError(message)


def verify_report(path):
    path = Path(path).absolute()
    require(path.resolve() == path and path.is_relative_to((ROOT / 'target').resolve()),
            'queue evidence must be a regular checkout target path without symlinks')
    report = json.loads(path.read_text())
    require(report['schema'] == 'fireweed.queue_pg_execution.v1'
            and report['scope'] == 'correctness_only_not_tp005', 'queue evidence schema/scope')
    require(report['status'] == 'PASS' and report['cleanup'] is True
            and report['cleanup_errors'] == [], 'queue execution or cleanup incomplete')
    require(report['revision'] == subprocess.check_output(
        ['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(), 'queue evidence HEAD is stale')
    require(report['sources'] == sources(), 'queue evidence source/lock/route hashes are stale')
    require(report['postgres_version'].startswith('postgres (PostgreSQL) 16.'),
            'queue evidence must execute PostgreSQL 16')
    qualification = report['postgres_qualification']
    require(qualification['current_user'] == 'fireweed_queue'
            and qualification['data_directory_verified'] is True
            and qualification['server_version_num'] // 10000 == 16
            and qualification['privileges'] == dict.fromkeys(
                ('superuser', 'createdb', 'createrole', 'replication', 'bypassrls'), False)
            and qualification['durability'] == dict.fromkeys(
                ('fsync', 'synchronous_commit', 'full_page_writes'), 'on'),
            'PostgreSQL limited-role/durability qualification missing')
    require(isinstance(report['reaped_adopted'], list), 'missing adopted-daemon drainage')
    attestation = report['s3_attestation']
    require(attestation['results']['selected'] is True
            and attestation['s3']['provider'] == 'rustfs'
            and attestation['s3']['version'] == '1.0.0'
            and attestation['s3']['artifact_sha256'] == 'c30a95b76546f25122c9ca387090ddb30c391ca5605621b0d7c881703c0f21c8'
            and attestation['preflight']['status'] == 'passed'
            and attestation['cleanup']['status'] == 'completed', 'S3 fixture not qualified/cleaned')

    def log(name, checksum):
        require(isinstance(name, str) and Path(name).name == name, 'log must be a basename')
        candidate = path.parent / name
        require(not candidate.is_symlink() and candidate.is_file(), 'log missing or symlinked')
        descriptor = os.open(candidate, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, 'rb') as stream:
            require(stat.S_ISREG(os.fstat(stream.fileno()).st_mode), 'log must be a regular file')
            raw = stream.read()
        require(hashlib.sha256(raw).hexdigest() == checksum, 'raw log digest changed')
        return raw.decode('utf-8')

    routes = load_routes(ROOT)
    expected = [r for r in routes if r['kind'] != 'child']
    require(len(report['routes']) == len(expected), 'missing/extra execution routes')
    for entry, observed in zip(expected, report['routes'], strict=True):
        command = cargo_route(entry)
        covered = [entry['debt_id']] + [r['debt_id'] for r in routes
            if r.get('parent_debt_id') == entry['debt_id']]
        require(observed['debt_ids'] == covered and observed['command'] == command
                and observed['listing_command'] == command + ['--list', '--format', 'terse']
                and observed['execution_command'] == command + ['--nocapture'],
                'executed debt/selector/argv mismatch')
        require(observed['listing'] == entry['debt_id'] + '.list.log', 'noncanonical listing filename')
        validate_listing(entry, log(observed['listing'], observed['listing_sha256']))
        require(observed['log'] == entry['debt_id'] + '.run.log', 'noncanonical execution filename')
        validate_execution(entry, log(observed['log'], observed['log_sha256']))
    children = [r for r in routes if r['kind'] == 'child']
    require(len(report['child_listings']) == len(children), 'missing/extra child compiled listings')
    for entry, observed in zip(children, report['child_listings'], strict=True):
        require(observed['debt_id'] == entry['debt_id'] and observed['command'] ==
                cargo_route(entry) + ['--list', '--format', 'terse'], 'child compiled selector mismatch')
        require(observed['listing'] == entry['debt_id'] + '.list.log', 'noncanonical listing filename')
        validate_listing(entry, log(observed['listing'], observed['listing_sha256']))
    require(set(report['fixture_logs']) == {'s3-provision.log', 's3-isolation.log', 's3-teardown.log'},
            'missing/extra fixture logs')
    for name, checksum in report['fixture_logs'].items():
        log(name, checksum)
    return {entry['debt_id'] for entry in routes}
