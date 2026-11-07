"""Independent owner-path execution and evidence failure controls."""
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch
import check_owner as bootstrap
gate = bootstrap.load_runtime()
model = gate.model

CATCH_UP = ('CatchUp', 'ReadCatchUp', 'ResolveCatchUp', 'RebuildCatchUp')
ENVIRONMENT = ('ForeignFence', 'ForeignAppend', 'InFlightLands', 'InFlightExpires', 'Crash', 'ReadHead')


def walk(actions, spec=None):
    spec = spec or model.Owner()
    state = spec.initial
    for wanted in actions:
        state = next(after for action, after in spec.transitions(state) if action == wanted)
    return state


def protocol(state, spec=None):
    spec = spec or model.Owner()
    return [action for action, _ in spec.transitions(state) if action not in ENVIRONMENT]


FIRST = ['OpenEmpty', 'BeginWrite:a', 'AcquireFence', 'CatchUp', 'Plan']


class OwnerGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.report = gate.run()

    def test_complete_graph_and_replayable_mutants(self):
        self.assertEqual(self.report['schema'], bootstrap.SCHEMA)
        baseline = self.report['baseline'][0]
        self.assertTrue(baseline['complete'])
        self.assertGreater(baseline['frontiers'], 0)
        self.assertEqual(set(baseline['completed_witnesses']), set(gate.WITNESSES))
        self.assertEqual({c['fault'] for c in self.report['negative_controls']}, set(model.FAULTS))
        for control in self.report['negative_controls']:
            self.assertEqual(control['property'], model.FAULTS[control['fault']])
            gate.helper.replay(model.Owner(control['fault']), control['trace'], control['property'])

    def test_required_oracles_each_have_a_faulty_control(self):
        self.assertEqual(set(model.FAULTS.values()),
                         {'O-ACK', 'O-FENCE', 'O-UNRESOLVED', 'O-TRUST', 'O-UNIQUE', 'O-PREFIX'})

    def test_healthy_second_write_needs_no_fence_or_log_read(self):
        state = walk(FIRST + ['PublishAccepted', 'ApplyAndAck', 'BeginWrite:b'])
        self.assertEqual(protocol(state), ['Plan'])
        actions = ['Plan', 'PublishAccepted', 'ApplyAndAck']
        state = walk(FIRST + ['PublishAccepted', 'ApplyAndAck', 'BeginWrite:b'] + actions)
        self.assertEqual((state.log, state.projection, state.fence), (('a', 'b'), ('a', 'b'), 1))

    def test_covered_read_serves_without_lock_or_log_read(self):
        state = walk(FIRST + ['PublishAccepted', 'ApplyAndAck', 'ReadHead'])
        self.assertEqual([a for a, _ in model.Owner().reading(state)], ['ServeFast'])

    def test_read_catches_up_only_to_the_captured_head(self):
        state = walk(['OpenEmpty', 'ForeignFence', 'ForeignAppend', 'ReadHead', 'ReadLock'])
        self.assertEqual([a for a, _ in model.Owner().reading(state)], ['ReadCatchUp'])
        state = walk(['ReadCatchUp'], _At(state))
        self.assertEqual(state.projection[:state.read_head], ('f',))
        self.assertEqual(len(state.projection), state.read_head)

    def test_superseded_fence_cannot_commit(self):
        state = walk(FIRST)
        state = walk(['ForeignFence'], _At(state))
        self.assertEqual(protocol(state), ['PublishRejected', 'PublishInFlight'])
        rejected = walk(['PublishRejected'], _At(state))
        self.assertEqual((rejected.held, rejected.log, protocol(rejected)), (0, (), ['AcquireFence']))

    def test_publish_conditions_check_epoch_and_exact_offset_independently(self):
        spec = model.Owner()
        state = model.State(log=('f',), fence=2)
        self.assertTrue(spec.accepted(state, 2, 1))
        self.assertFalse(spec.accepted(state, 1, 1))
        self.assertFalse(spec.accepted(state, 2, 0))
        self.assertFalse(spec.accepted(state, 2, 2))

    def test_lost_reply_returns_the_durable_outcome_without_replanning(self):
        state = walk(FIRST + ['PublishLostReply'])
        self.assertEqual((state.unresolved, protocol(state)), ('a', ['ResolveFence']))
        state = walk(['ResolveFence', 'ResolveCatchUp'], _At(state))
        self.assertGreater(state.held, 1)
        self.assertEqual(protocol(state), ['ResolvePresent'])
        state = walk(['ResolvePresent'], _At(state))
        self.assertEqual((state.log, state.acknowledged, state.unresolved), (('a',), ('a',), ''))

    def test_strictly_newer_fence_kills_the_late_publish(self):
        state = walk(FIRST + ['PublishInFlight', 'ResolveFence'])
        self.assertEqual(state.inflight, ('a', 1, 0))
        self.assertNotIn('InFlightLands', [a for a, _ in model.Owner().environment(state)])
        self.assertEqual(protocol(walk(['ResolveCatchUp'], _At(state))), ['ResolveAbsent'])
        mutant = model.Owner('same_fence_resolve')
        same = walk(FIRST + ['PublishInFlight', 'ResolveFence'], mutant)
        self.assertIn('InFlightLands', [a for a, _ in mutant.environment(same)])

    def test_lost_reply_without_lookup_duplicates_the_identity(self):
        mutant = model.Owner('skip_identity_lookup')
        state = walk(FIRST + ['PublishLostReply', 'ResolveFence', 'ResolveCatchUp', 'ResolveAbsent', 'Plan'], mutant)
        after = next(a for action, a in mutant.transitions(state) if action == 'PublishAccepted')
        self.assertEqual(mutant.violation(state, 'PublishAccepted', after), 'O-UNIQUE')

    def test_missing_sidecar_discards_the_projection(self):
        crashed = walk(FIRST + ['PublishAccepted', 'ApplyAndAck', 'Crash'])
        self.assertEqual((crashed.projection, crashed.tainted, crashed.sidecar), (('a',), True, False))
        self.assertEqual(protocol(crashed), ['OpenEmpty'])
        reopened = walk(['OpenEmpty'], _At(crashed))
        self.assertEqual((reopened.projection, reopened.tainted, reopened.held), ((), False, 0))

    def test_open_deletes_the_sidecar_before_any_write(self):
        state = walk(FIRST + ['PublishAccepted', 'ApplyAndAck', 'Shutdown'])
        self.assertEqual((state.sidecar, protocol(state)), (True, ['OpenTrusted']))
        state = walk(['OpenTrusted'], _At(state))
        self.assertEqual((state.sidecar, state.projection, state.held), (False, ('a',), 0))

    def test_poison_blocks_every_serve_until_genesis_rebuild_finishes(self):
        state = walk(['OpenEmpty', 'ReadHead'] + FIRST[1:] + ['PublishAccepted', 'ApplyFails'])
        spec = model.Owner()
        self.assertEqual(list(spec.reading(state)), [])
        for action in ('PoisonRebuild', 'RebuildCatchUp'):
            self.assertEqual(list(spec.reading(state)), [])
            state = walk([action], _At(state))
        self.assertEqual((state.projection, state.poisoned, state.tainted), (('a',), False, False))
        self.assertEqual([a for a, _ in spec.reading(state)], ['ServeFast'])

    def test_later_read_observes_acknowledged_write_after_crash(self):
        state = walk(FIRST + ['PublishAccepted', 'ApplyAndAck', 'Crash', 'OpenEmpty',
                              'ReadHead', 'ReadLock', 'ReadCatchUp', 'ServeLocked'])
        self.assertEqual(state.projection, ('a',))

    def test_every_reachable_state_can_settle(self):
        specification = model.Owner()
        _, (states, edges, _) = gate.helper.explore(specification, 100000)
        self.assertEqual(gate.stuck_states(states, edges), 0)
        edges = [[(a, d) for a, d in row if not a.startswith(('Plan', 'ResolveFence'))] for row in edges]
        self.assertGreater(gate.stuck_states(states, edges), 0)

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
        edges = [[(a, d) for a, d in row if a != 'PublishAccepted'] for row in edges]
        original = gate.helper.explore
        with patch.object(gate.helper, 'explore',
                          lambda m, cap: (report, (states, edges, parents)) if m.fault is None else original(m, cap)):
            with self.assertRaises(AssertionError):
                gate.run()

    def test_insensitive_control_fails_the_gate(self):
        original = gate.helper.explore
        def blind(spec, cap):
            return original(model.Owner() if spec.fault == 'skip_head' else spec, cap)
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


class _At(model.Owner):
    """Continue a walk from an explicit state of the unfaulted model."""
    def __init__(self, state):
        super().__init__()
        self.initial = state


if __name__ == '__main__':
    unittest.main()
