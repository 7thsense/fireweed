"""Exercise the qualification entry point without running a storage workload."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class QualificationRunnerTests(unittest.TestCase):
    def run_runner(self, fail_campaign):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scripts = root / 'scripts' / 'perf'
            scripts.mkdir(parents=True)
            runner = scripts / 'qualify-workflow-capacity.sh'
            shutil.copyfile(Path(__file__).with_name(runner.name), runner)
            binaries = root / 'bin'
            binaries.mkdir()
            (binaries / 'cargo').write_text('#!/bin/sh\nexit 0\n')
            # Absolute interpreter avoids recursively invoking this stub.
            import sys
            (binaries / 'python3').write_text(
                '#!' + sys.executable + '\nimport json,os,sys\n'
                'with open(os.environ["CALLS"], "a") as f: f.write(json.dumps(sys.argv[1:])+"\\n")\n'
                'print("{}")\n'
                'sys.exit(1 if os.environ["FAIL_CAMPAIGN"] == "1" and "campaign" in sys.argv else 0)\n')
            for path in binaries.iterdir():
                path.chmod(0o755)
            calls = root / 'calls.jsonl'
            env = dict(os.environ, PATH=str(binaries) + os.pathsep + os.environ['PATH'],
                       CALLS=str(calls), FAIL_CAMPAIGN=str(int(fail_campaign)))
            result = subprocess.run(['bash', str(runner), str(root / 'results')],
                                    env=env, capture_output=True, text=True)
            return result, [json.loads(line) for line in calls.read_text().splitlines()]

    def test_representative_campaign_and_varied_primitives_repeated(self):
        result, calls = self.run_runner(False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([c[c.index('--profile') + 1] for c in calls],
                         ['campaign', 'primitives', 'campaign', 'primitives'])
        for call in calls:
            self.assertEqual(call[call.index('--items') + 1], '1000000')
            self.assertNotIn('--memory', call)
            self.assertNotIn('--no-faults', call)
            if 'campaign' in call:
                for flag in ('--campaign-metadata', '--campaign-timestamp-priority', '--recycle', '--stretch'):
                    self.assertIn(flag, call)
                self.assertEqual(call[call.index('--cycles') + 1], '8')
            else:
                self.assertIn('--primitive-varied-payload', call)
                self.assertIn('--qualify', call)

    def test_failure_is_preserved_after_subsequent_successes(self):
        result, calls = self.run_runner(True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(calls), 4)
