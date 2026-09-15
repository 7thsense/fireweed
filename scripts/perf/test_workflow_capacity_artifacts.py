"""Exercise capacity-run cleanup with a fake CLI; these are not performance tests."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


class CapacityArtifactsTests(unittest.TestCase):
    def run_fixture(self, directory, exit_code, explicit_root=False):
        repo = Path(directory)
        scripts = repo / "scripts/perf"
        scripts.mkdir(parents=True)
        for name in ("workflow-capacity.py", "workflow_storage_monitor.py"):
            shutil.copyfile(Path(__file__).with_name(name), scripts / name)
        binary = repo / "target/release/fireweed-workload"
        binary.parent.mkdir(parents=True)
        binary.write_text("""#!/usr/bin/env python3
import json, sys
from pathlib import Path
root = Path(sys.argv[sys.argv.index('--root') + 1])
root.mkdir(parents=True, exist_ok=True)
(root / 'log-marker').write_text('inspectable failure evidence')
print(json.dumps({'fixture': True}))
raise SystemExit(int(sys.argv[sys.argv.index('--exit-code') + 1]))
""")
        binary.chmod(0o700)
        subprocess.run(["git", "init", "-q", str(repo)], check=True)
        subprocess.run(["git", "-C", str(repo), "-c", "user.name=Fixture",
                        "-c", "user.email=fixture@example.invalid", "commit",
                        "--allow-empty", "-qm", "fixture"], check=True)
        command = [sys.executable, str(scripts / "workflow-capacity.py"),
                   "--profile", "fixture", "--exit-code", str(exit_code)]
        if explicit_root:
            command += ["--root", str(repo / "explicit-data")]
        child = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(child.returncode, exit_code, child.stderr)
        return json.loads(child.stdout)

    def test_failed_automatic_root_survives_process_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            report = self.run_fixture(directory, 7)
            command = report["command"]
            original_root = command[command.index("--root") + 1]
            root = Path(report.get("retained_data_root", original_root))
            self.assertTrue((root / "log-marker").is_file(), "failed run log was deleted")
            self.assertTrue(root.name.startswith("failed-run-"))
            self.assertEqual((root / "log-marker").read_text(), "inspectable failure evidence")
            self.assertFalse(Path(report["original_data_root"]).exists())
            self.assertEqual(report["exit_code"], 7)
            self.assertNotIn("result", report)

    def test_successful_automatic_root_is_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            report = self.run_fixture(directory, 0)
            self.assertNotIn("retained_data_root", report)
            self.assertEqual(report["result"], {"fixture": True})
            command = report["command"]
            self.assertFalse(Path(command[command.index("--root") + 1]).exists())

    def test_failed_explicit_root_stays_at_requested_path(self):
        with tempfile.TemporaryDirectory() as directory:
            report = self.run_fixture(directory, 9, explicit_root=True)
            self.assertEqual(report["retained_data_root"], str(Path(directory) / "explicit-data"))
            self.assertNotIn("original_data_root", report)
            self.assertTrue((Path(report["retained_data_root"]) / "log-marker").is_file())


if __name__ == "__main__":
    unittest.main()
