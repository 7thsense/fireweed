"""API-007 gate controls; legacy verification remains independently runnable."""
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import subprocess
import sys
from unittest.mock import patch
import unittest

from api007_models import Attempt, Caller, ModelBoundError, Publication, State
from check_api007 import output_path, replay, run, validate_loaded_sources, LOADED_SOURCES
from explorer import SearchLimit, render


class Api007GateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.report = run()

    def test_completed_positive_and_negative_traces(self):
        self.assertEqual(len(self.report['baseline']), 3)
        self.assertEqual(len(self.report['negative_controls']), 9)
        for baseline in self.report['baseline']:
            self.assertTrue(baseline['complete'])
            self.assertTrue(baseline['completed_witnesses'])
            self.assertEqual(baseline['frontiers'], 0)

    def test_failed_rerun_invalidates_previous_success(self):
        root = Path(__file__).resolve().parents[2]
        output = root / 'target/api007-formal/test-repeated-output.json'
        try:
            command = [sys.executable, str(root / 'scripts/formal/check_api007.py'), '--output', str(output)]
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 0)
            self.assertEqual(json.loads(output.read_text())['status'], 'PASS')
            self.assertEqual(subprocess.run(command + ['--max-states', '1'], capture_output=True).returncode, 1)
            self.assertEqual(json.loads(output.read_text())['status'], 'FAIL')
        finally:
            output.unlink(missing_ok=True)

    def test_loaded_executable_source_mismatch_fails(self):
        name = next(iter(LOADED_SOURCES))
        with patch.dict(LOADED_SOURCES, {name: '0' * 64}):
            with self.assertRaises(ValueError):
                validate_loaded_sources()

    def test_incomplete_search_is_failure(self):
        with self.assertRaises(SearchLimit):
            run(1)

    def test_unknown_configuration_and_mutant_rejected(self):
        with self.assertRaises(ValueError):
            Publication('typo')
        with self.assertRaises(ValueError):
            Publication('original-resolution', 'typo')

    def test_wrong_classification_oracle(self):
        model = Publication('original-resolution')
        trace = self.report['baseline'][0]['completed_witnesses']['equal_replay']['trace']
        # Reconstruct the reachable state immediately before its final Resolve.
        state = model.initial
        for row in trace[1:-1]:
            state = next(after for action, after in model.transitions(state)
                         if action == row['action'] and json.loads(json.dumps(render(after))) == json.loads(json.dumps(row['state'])))
        action, after = next((action, after) for action, after in model.transitions(state)
                             if action == 'Resolve:0:replayed')
        wrong = replace(after, callers=(replace(after.callers[0], result='conflict'), after.callers[1]))
        self.assertEqual(model.violation(state, action, wrong), 'F-REPLAY')

    def test_trace_damage_rejected(self):
        model = Publication('original-resolution')
        original = self.report['baseline'][0]['completed_witnesses']['equal_replay']['trace']
        for damage in ('action', 'state', 'extra'):
            trace = json.loads(json.dumps(original))
            if damage == 'action':
                trace[-1]['action'] = 'invented'
            elif damage == 'state':
                trace[-1]['state']['epoch'] += 1
            else:
                trace[-1]['extra'] = True
            with self.assertRaises(ValueError):
                replay(model, trace)

    def test_submission_required_for_storage(self):
        model = Publication('original-resolution')
        _, prepared = next((a, s) for a, s in model.transitions(model.initial) if a == 'Prepare:0:0')
        self.assertFalse(any(a.startswith('CASStore:') for a, _ in model.transitions(prepared)))
        _, dispatched = next((a, s) for a, s in model.transitions(prepared) if a == 'Dispatch:0')
        self.assertTrue(any(a == 'CASStore:0' for a, _ in model.transitions(dispatched)))

    def test_full_history_still_rejects_stale_pending_attempt(self):
        model = Publication('background-publication')
        state = State(history=((0, 0, 1), (1, 0, 2)), epoch=4,
                      attempts=(Attempt(0, 0, 0, 2, 0, 'pending'),))
        self.assertTrue(any(a == 'CASReject:0' for a, _ in model.transitions(state)))
        state = replace(state, attempts=(Attempt(0, 0, 0, 4, 2, 'pending'),))
        with self.assertRaises(ModelBoundError):
            list(model.transitions(state))

    def test_output_escape_and_symlink_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.assertEqual(output_path(root, Path('target/api007-formal/report.json')),
                             root / 'target/api007-formal/report.json')
            for path in (Path('report.json'), Path('target/api007-formal/../../../escape.json')):
                with self.assertRaises(ValueError):
                    output_path(root, path)
            (root / 'target').mkdir()
            (root / 'target/api007-formal').symlink_to(root)
            with self.assertRaises(ValueError):
                output_path(root, Path('target/api007-formal/report.json'))


if __name__ == '__main__':
    unittest.main()
