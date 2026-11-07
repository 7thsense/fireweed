"""Independent lease-owner execution and evidence failure controls."""
import json
from dataclasses import replace
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch
import check_owner as bootstrap
gate = bootstrap.load_runtime()
model = gate.model

ENTER = gate.ENTER
TAKEOVER = gate.TAKEOVER


def walk(actions, spec=None):
    """Follow named actions exactly, one enabled transition each."""
    spec = spec or model.Owner()
    state = spec.initial
    for wanted in actions:
        state = next(after for action, after in spec.transitions(state) if action == wanted)
    return state


def enabled(state, spec=None):
    return [action for action, _ in (spec or model.Owner()).transitions(state)]


# Entry with the minimum clock movement: one tick to wait out the skew allowance.
ENTRY = ['Acquire0', 'Process0', 'Granted0', 'Tick', 'SkewDone0', 'Fence0', 'Confirm0']
SERVING = walk(ENTRY)


class At(model.Owner):
    """Continue a walk from an explicit state."""
    def __init__(self, state, fault=None, scenario=None):
        super().__init__(fault, scenario)
        self.initial = state


class OwnerGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.report = gate.run()

    def test_complete_graphs_and_replayable_mutants(self):
        self.assertEqual(self.report['schema'], bootstrap.SCHEMA)
        self.assertEqual({b['scenario'] for b in self.report['baseline']}, set(model.SCENARIOS))
        for baseline in self.report['baseline']:
            self.assertTrue(baseline['complete'])
            self.assertGreater(baseline['frontiers'], 0)
        witnesses = self.report['baseline'][0]['completed_witnesses']
        self.assertEqual(set(witnesses), set(gate.WITNESSES))
        self.assertEqual({c['fault'] for c in self.report['negative_controls']}, set(model.FAULTS))
        for control in self.report['negative_controls']:
            self.assertEqual(control['property'], model.FAULTS[control['fault']])
            mutant = model.Owner(control['fault'])
            gate.helper.replay(mutant, control['trace'], control['property'])

    def test_required_faulty_controls_exist_for_every_oracle(self):
        self.assertEqual(set(model.FAULTS.values()),
                         {'O-READ', 'O-FENCE', 'O-UNRESOLVED', 'O-TRUST', 'O-UNIQUE'})
        for fault in ('serve_past_deadline', 'deadline_from_after_send', 'new_owner_skips_skew',
                      'keep_projection_after_publish_failure', 'ack_without_recheck'):
            self.assertIn(fault, model.FAULTS)

    def test_deadline_is_measured_from_the_send_not_the_reply(self):
        state = walk(['Acquire0', 'Process0', 'Tick', 'Tick'])
        reply = next(a for action, a in model.Owner().transitions(state) if action == 'Granted0')
        self.assertEqual(reply.owners[0].candidate, 2)
        late = model.Owner('deadline_from_after_send')
        reply = next(a for action, a in late.transitions(state) if action == 'Granted0')
        self.assertEqual(reply.owners[0].candidate, 4)

    def test_a_new_owner_waits_out_the_skew_allowance(self):
        state = walk(['Acquire0', 'Process0', 'Granted0'])
        self.assertNotIn('SkewDone0', enabled(state))
        state = walk(['Tick'], At(state))
        self.assertIn('SkewDone0', enabled(state))
        eager = model.Owner('new_owner_skips_skew')
        self.assertIn('SkewDone0', enabled(walk(['Acquire0', 'Process0', 'Granted0'], eager), eager))

    def test_a_passed_deadline_exits_with_no_contact(self):
        state = walk(['Tick', 'Tick'], At(SERVING))
        self.assertGreaterEqual(state.now, state.owners[0].deadline)
        self.assertIn('Exit0', enabled(state))
        left = walk(['Exit0'], At(state))
        self.assertEqual((left.owners[0].phase, left.owners[0].proj), ('out', ()))

    def test_a_read_is_acked_only_before_the_deadline(self):
        state = walk(['ReadTake0', 'ReadEval0'], At(SERVING))
        self.assertIn('ReadAck0', enabled(state))
        while state.now < state.owners[0].deadline:
            state = walk(['Tick'], At(state))
        self.assertNotIn('ReadAck0', enabled(state))
        blind = model.Owner('ack_without_recheck')
        self.assertIn('ReadAck0', enabled(state, blind))

    def test_superseded_fence_cannot_commit(self):
        state = walk(['Tick', 'Tick', 'Exit0'] + TAKEOVER[:3] + ['Tick'] + TAKEOVER[3:], At(SERVING))
        self.assertEqual(state.fence, 2)
        stale = SERVING
        self.assertEqual(stale.owners[0].fence, 1)
        # The exited owner re-enters with a newer fence; its old fence is gone.
        self.assertEqual(state.owners[0].fence, 0)

    def test_publish_checks_epoch_and_exact_offset_independently(self):
        newer = replace(SERVING, fence=2)
        self.assertEqual([a for a in enabled(newer) if a.startswith('Publish:')], ['Publish:a0'] * 0
                         or [a for a in enabled(newer) if a.startswith('Publish:')])
        self.assertNotIn('Publish:a0', enabled(newer))
        self.assertIn('PublishRejected:a0', enabled(newer))
        ahead = replace(SERVING, log=(('x', 1),))
        self.assertNotIn('Publish:a0', enabled(ahead))
        blind = model.Owner('ignore_fence')
        after = next(a for action, a in blind.transitions(newer) if action == 'Publish:a0')
        self.assertEqual(blind.violation(newer, 'Publish:a0', after), 'O-FENCE')

    def test_failed_publish_leaves_owner_state(self):
        state = walk(['PublishLostReply:a0'], At(SERVING))
        self.assertTrue(state.owners[0].unresolved)
        self.assertEqual([a for a in enabled(state) if a.endswith('0') and a != 'Exit0'
                          and not a.startswith(('Acquire', 'Process', 'Lose'))], [])
        keep = model.Owner('keep_projection_after_publish_failure')
        replanned = next(a for action, a in keep.transitions(state) if action.startswith('PublishRejected'))
        self.assertEqual(keep.violation(state, 'PublishRejected:a0', replanned), 'O-UNRESOLVED')
        left = walk(['Exit0'], At(state))
        self.assertEqual((left.owners[0].phase, left.owners[0].proj, left.owners[0].unresolved),
                         ('out', (), False))
        # Re-entry rebuilds from the log, so the published original is found by identity.
        again = walk(['Acquire0', 'Process0', 'Granted0', 'Tick', 'SkewDone0', 'Fence0'], At(left))
        self.assertEqual(again.owners[0].proj, ('a',))
        self.assertIn('ReplayAck:a0', enabled(walk(['Confirm0'], At(again))))

    def test_crash_discards_the_projection(self):
        spec = model.Owner(scenario='crash')
        crashed = walk(ENTRY + ['Publish:a0', 'Crash0'], spec)
        self.assertEqual((crashed.owners[0].phase, crashed.owners[0].proj), ('out', ()))
        kept = model.Owner('retain_projection_across_crash')
        crashed = walk(ENTRY + ['Publish:a0', 'Crash0'], kept)
        self.assertEqual(crashed.owners[0].proj, ('a',))

    def test_untrusted_projection_cannot_confirm(self):
        kept = model.Owner('retain_projection_across_crash')
        state = walk(ENTRY + ['Publish:a0', 'Crash0', 'Acquire0', 'Process0', 'Granted0', 'Tick',
                              'SkewDone0', 'Fence0'], kept)
        after = next(a for action, a in kept.transitions(state) if action == 'Confirm0')
        self.assertEqual(kept.violation(state, 'Confirm0', after), 'O-TRUST')

    def test_lost_renewal_reply_keeps_the_deadline(self):
        spec = model.Owner(scenario='lost')
        state = walk(ENTRY + ['Renew0', 'Process0', 'LoseReply0'], spec)
        self.assertEqual(state.owners[0].deadline, 2)
        spec = model.Owner(scenario='renew')
        state = walk(ENTRY + ['Renew0', 'Process0', 'Renewed0'], spec)
        self.assertGreater(state.owners[0].deadline, 2)

    def test_every_reachable_state_can_settle(self):
        for scenario, limits in model.SCENARIOS.items():
            specification = model.Owner(scenario=scenario)
            _, (states, edges, _) = gate.helper.explore(specification, 100000)
            self.assertEqual(gate.stuck_states(states, edges, limits['horizon']), 0)
        edges = [[(a, d) for a, d in row if not a.startswith('Process')] for row in edges]
        self.assertGreater(gate.stuck_states(states, edges, limits['horizon']), 0)

    def test_incomplete_search_cannot_pass(self):
        with self.assertRaises(gate.helper.SearchLimit):
            gate.run(1)

    def test_damaged_trace_cannot_replay(self):
        control = self.report['negative_controls'][0]
        trace = json.loads(json.dumps(control['trace']))
        trace[-1]['state']['fence'] += 99
        with self.assertRaises(ValueError):
            gate.helper.replay(model.Owner(control['fault']), trace, control['property'])

    def test_missing_publication_cannot_complete_named_witness(self):
        spec = model.Owner()
        report, graph = gate.helper.explore(spec, 100000)
        states, edges, parents = graph
        edges = [[(a, d) for a, d in row if not a.startswith('Publish')] for row in edges]
        original = gate.helper.explore
        with patch.object(gate.helper, 'explore',
                          lambda m, cap: (report, (states, edges, parents))
                          if m.fault is None and m.scenario == 'base' else original(m, cap)):
            with self.assertRaises(AssertionError):
                gate.run()

    def test_insensitive_control_fails_the_gate(self):
        original = gate.helper.explore
        def blind(spec, cap):
            return original(model.Owner() if spec.fault == 'ack_without_recheck' else spec, cap)
        with patch.object(gate.helper, 'explore', blind):
            with self.assertRaises(AssertionError):
                gate.run()

    def test_failed_rerun_replaces_previous_pass(self):
        root = Path(__file__).resolve().parents[2]
        output = root / 'target/api007-formal/test-owner-rerun.json'
        command = [sys.executable, str(root / 'scripts/formal/check_owner.py'), '--output', str(output)]
        try:
            for suffix, status in [([], 0), (['--max-states', '1'], 1), ([], 0),
                                   (['--max-states', 'bad'], 1), ([], 0), (['--out', str(output)], 1)]:
                self.assertEqual(subprocess.run(command + suffix, capture_output=True).returncode, status)
                self.assertEqual(json.loads(output.read_text())['status'], 'PASS' if status == 0 else 'FAIL')
        finally:
            output.unlink(missing_ok=True)

    def test_output_escape_and_unknown_fault_are_refused(self):
        root = Path(__file__).resolve().parents[2]
        with self.assertRaises(ValueError):
            bootstrap.output_path(root, Path('/tmp/owner-proof.json'))
        with self.assertRaises(ValueError):
            model.Owner('typo')

    def test_executed_runner_body_is_captured(self):
        path = str(Path(gate.__file__).resolve())
        self.assertIn(path, gate.helper.LOADED_SOURCES)
        with patch.dict(gate.helper.LOADED_SOURCES, {path: '0' * 64}):
            with self.assertRaises(ValueError):
                gate.helper.validate_loaded_sources()

    def test_startup_missing_or_invalid_source_replaces_previous_pass(self):
        root = Path(__file__).resolve().parents[2]
        output = root / 'target/api007-formal/test-owner-startup.json'
        read = Path.read_bytes
        for dependency in ('owner_gate.py', 'check_api007.py', 'owner_models.py'):
            for invalid in (False, True):
                try:
                    self.assertEqual(bootstrap.main(['--output', str(output)]), 0)
                    def damaged(path):
                        if path.name == dependency:
                            if invalid:
                                return b'this is not valid Python!'
                            raise FileNotFoundError('injected unavailable dependency')
                        return read(path)
                    with patch.object(Path, 'read_bytes', damaged):
                        self.assertEqual(bootstrap.main(['--output', str(output)]), 1)
                    self.assertEqual(json.loads(output.read_text())['status'], 'FAIL')
                finally:
                    output.unlink(missing_ok=True)


if __name__ == '__main__':
    unittest.main()
