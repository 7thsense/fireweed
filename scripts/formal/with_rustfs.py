#!/usr/bin/env python3
"""Run formal conformance against an isolated task-owned loopback RustFS."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--workers', type=int, choices=range(1,17), default=1)
    parser.add_argument('--replay-profile', choices=('smoke', 'edges'))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    binary = args.binary.resolve(strict=True)
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        port = reservation.getsockname()[1]
    endpoint = f'http://127.0.0.1:{port}'
    access, secret = 'formal' + secrets.token_hex(8), secrets.token_hex(24)
    env = dict(os.environ, RUSTFS_ACCESS_KEY=access, RUSTFS_SECRET_KEY=secret,
               RUSTFS_CONSOLE_ENABLE='false', FIREWEED_S3_TEST_ENDPOINT=endpoint,
               FIREWEED_S3_TEST_BUCKET='fireweed-formal', FIREWEED_S3_TEST_REGION='us-east-1',
               FIREWEED_S3_TEST_ACCESS_KEY=access, FIREWEED_S3_TEST_SECRET_KEY=secret)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='fireweed-formal-s3-') as directory:
        with open(Path(directory) / 'rustfs.log', 'w') as server_log:
            server = subprocess.Popen([str(binary), 'server', directory, '--address', f'127.0.0.1:{port}'],
                                      env=env, stdout=server_log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    if server.poll() is not None:
                        raise RuntimeError('task-owned RustFS exited before readiness')
                    try:
                        with urllib.request.urlopen(endpoint + '/health/ready', timeout=1) as response:
                            if response.status == 200:
                                break
                    except (urllib.error.URLError, TimeoutError):
                        pass
                    time.sleep(0.1)
                else:
                    raise RuntimeError('task-owned RustFS readiness timeout')
                preflight = args.output.with_name(args.output.stem + '-s3-cas.json')
                subprocess.run([sys.executable, str(root / 'scripts/ci/s3-native-cas-preflight.py'),
                                '--json-out', str(preflight)], cwd=root, env=env, check=True)
                runner = 'run_replay.py' if args.replay_profile else 'conformance.py'
                command = [sys.executable, str(root / 'scripts/formal' / runner),
                           '--output', str(args.output)]
                if args.replay_profile:
                    command += ['--profile', args.replay_profile, '--workers', str(args.workers)]
                if args.offline:
                    command.append('--offline')
                result = subprocess.run(command, cwd=root, env=env)
                report = json.loads(args.output.read_text())
                digest = hashlib.sha256()
                with binary.open('rb') as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b''):
                        digest.update(block)
                report['s3_fixture'] = {'endpoint': endpoint, 'console_enabled': False,
                                       'binary_sha256': digest.hexdigest(),
                                       'native_cas_evidence': str(preflight.resolve()),
                                       'lifetime': 'task-owned child terminated in finally'}
                with args.output.open('w') as output:
                    json.dump(report, output, sort_keys=True, separators=(',', ':'))
                    output.write('\n')
                return result.returncode
            finally:
                server.terminate()
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()


if __name__ == '__main__':
    sys.exit(main())
