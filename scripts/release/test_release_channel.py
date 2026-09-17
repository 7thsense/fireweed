#!/usr/bin/env python3
"""Behavioral publication-channel tests using real Git identities."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("release_channel", Path(__file__).with_name("release-channel.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ChannelTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.git("init", "-q")
        self.git("config", "user.name", "Test")
        self.git("config", "user.email", "test@example.invalid")
        (self.root / "Cargo.toml").write_text('[workspace.package]\nversion = "1.2.3"\n')
        self.commit()
        self.source = self.git("rev-parse", "HEAD")

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.root), *args], text=True, stderr=subprocess.DEVNULL).strip()

    def commit(self):
        self.git("add", ".")
        self.git("commit", "-qm", "fixture")

    def preview(self, **changes):
        path = self.root / "docs/releases"
        path.mkdir(parents=True)
        manifest = dict(schema="fireweed.source-preview-release.v1", version="1.2.3", measured_source=self.source,
                        claims={"governed_product_ready": False, "signed": False})
        manifest.update(changes)
        (path / "v1.2.3.source-preview.json").write_text(json.dumps(manifest))
        (path / "v1.2.3.md").write_text("Source preview, no governed deployment claim.\n")
        self.commit()
        self.git("tag", "v1.2.3")

    def test_absent_manifest_preserves_governed_gates(self):
        self.git("tag", "v1.2.3")
        self.assertEqual(module.resolve(self.root, "v1.2.3")["channel"], "governed")

    def test_preview_uses_distinct_measured_ancestor(self):
        self.preview()
        result = module.resolve(self.root, "v1.2.3")
        self.assertEqual(result["channel"], "source-preview")
        self.assertEqual(result["measured_source"], self.source)
        self.assertNotEqual(result["evidence_commit"], self.source)

    def test_preview_cannot_assert_readiness(self):
        self.preview(claims={"governed_product_ready": True, "signed": False})
        with self.assertRaises(ValueError):
            module.resolve(self.root, "v1.2.3")

    def test_wrong_package_version_rejected(self):
        (self.root / "Cargo.toml").write_text('[workspace.package]\nversion = "0.0.1"\n')
        self.commit()
        self.source = self.git("rev-parse", "HEAD")
        self.preview()
        with self.assertRaises(ValueError):
            module.resolve(self.root, "v1.2.3")

    def test_nonexistent_source_rejected(self):
        self.preview(measured_source="0" * 40)
        with self.assertRaises(subprocess.CalledProcessError):
            module.resolve(self.root, "v1.2.3")

    def test_ambient_head_cannot_replace_tag(self):
        self.preview()
        (self.root / "new").write_text("later")
        self.commit()
        with self.assertRaises(ValueError):
            module.resolve(self.root, "v1.2.3")


if __name__ == "__main__":
    unittest.main()
