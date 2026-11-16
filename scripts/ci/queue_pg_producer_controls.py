"""Real cancellation at the producer's provisioning and cleanup boundaries.

A temporary checkout and fake endpoint helper exercise orchestration only. These
controls cannot produce evidence accepted against the actual repository routes.
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

import queue_owned_process as lifecycle


def run():
    producer = Path(__file__).with_name('queue-pg-execution.py')
    for mode in ('provision', 'cleanup'):
        with tempfile.TemporaryDirectory(prefix='pg-producer-control-') as raw:
            root = Path(raw)
            helper = root / 'scripts/ci/s3-qualification-endpoint.sh'
            helper.parent.mkdir(parents=True)
            helper.write_text('''#!/bin/bash
set -eu
root="$FIREWEED_CONTROL_ROOT"
echo $$ >> "$root/pids"
echo "$FIREWEED_S3_SECRET_DIR" > "$root/secret-path"
case "$1" in
provision)
  mkdir -p "$FIREWEED_S3_SECRET_DIR/state"
  sleep 60 &
  echo $! >> "$root/pids"
  echo $! > "$FIREWEED_S3_SECRET_DIR/state/pid"
  if [[ "$FIREWEED_CONTROL_MODE" == provision ]]; then
    touch "$root/ready"
    sleep 60
  fi
  for suffix in ENDPOINT BUCKET ACCESS_KEY SECRET_KEY; do
    echo "FIREWEED_S3_TEST_${suffix}=control-only" >> "$FIREWEED_S3_SECRET_DIR/credentials.env"
  done
  echo '{}' > "$FIREWEED_S3_SECRET_DIR/s3-native-cas-capability-attestation.json"
  ;;
verify-isolation) ;;
teardown)
  if [[ "$FIREWEED_CONTROL_MODE" == cleanup ]]; then
    touch "$root/ready"
    sleep 1
  fi
  if [[ -f "$FIREWEED_S3_SECRET_DIR/state/pid" ]]; then
    kill "$(cat "$FIREWEED_S3_SECRET_DIR/state/pid")" 2>/dev/null || true
  fi
  ;;
esac
''')
            worker = root / 'worker.py'
            worker.write_text(f'''
import importlib.util,os,sys
from pathlib import Path
sys.path.insert(0,{str(producer.parent)!r})
import queue_owned_process as lifecycle
spec=importlib.util.spec_from_file_location('producer',{str(producer)!r})
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Fixture:
    def __init__(self,*_): self.cleaned=False;self.version='postgres (PostgreSQL) 16.control';self.qualification={{'control':True}}
    def start(self): pass
    def test_environment(self): return os.environ.copy()
    def close(self): self.cleaned=True
m.ROOT=Path({str(root)!r})
m.QueuePostgresFixture=Fixture
m.sources=lambda:{{'control':'isolated'}}
m.load_routes=lambda _:[]
m.subprocess.check_output=lambda *a,**k:'control-revision'
sys.argv=['producer','--pg-bin','/unused','--fixture-parent','/tmp','--output',{str(root / 'target/evidence')!r}]
lifecycle.own_descendants()
m.main()
''')
            environment = dict(os.environ, FIREWEED_CONTROL_ROOT=str(root), FIREWEED_CONTROL_MODE=mode)
            with lifecycle.owned_process([sys.executable, str(worker)], env=environment,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as process:
                deadline = time.monotonic() + 15
                while not (root / 'ready').exists():
                    lifecycle.require(process.poll() is None and time.monotonic() < deadline,
                                      'producer control did not reach cancellation boundary')
                    time.sleep(0.01)
                process.send_signal(signal.SIGTERM)
                time.sleep(0.1)
                process.send_signal(signal.SIGTERM)
                process.send_signal(signal.SIGINT)
                output, _ = process.communicate(timeout=15)
                lifecycle.require(process.returncode != 0, 'cancelled producer succeeded')
            report = json.loads((root / 'target/evidence/report.json').read_text())
            lifecycle.require(report['status'] == 'INCOMPLETE' and report['cleanup'] is True,
                              f'cancelled producer did not preserve incomplete/cleaned evidence: {output}')
            for pid in (root / 'pids').read_text().splitlines():
                lifecycle.require(not Path('/proc', pid).exists(), f'producer cancellation leaked {pid}')
            lifecycle.require(not Path((root / 'secret-path').read_text().strip()).exists(),
                              'producer retained unexpected secret state')
    print('PASS: producer provisioning and cleanup repeated-cancellation controls', flush=True)


if __name__ == '__main__':
    lifecycle.own_descendants()
    run()
    lifecycle.drain_adopted()
