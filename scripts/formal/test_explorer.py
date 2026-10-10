import unittest
import os
import subprocess
import sys
from pathlib import Path
from explorer import SearchLimit, explore, fair_lasso
from models import Model, Recovery, Batching, Admission, EligibleAge, Lifecycle


class Graph(Model):
    name = 'checker-fixture'
    initial = 0
    bounds = {'vertices': 3}
    properties = ['eventual-done']
    assumptions = ['weak fairness of Serve']
    fair_labels = frozenset({'Serve'})

    def __init__(self, edges, pending=lambda s: s != 2):
        self.edges = edges
        self.obligations = (('old-command', pending),)

    def transitions(self, s):
        return self.edges.get(s, [])

    def violation(self, before, action, after):
        return None


class CheckerTests(unittest.TestCase):
    def report(self, graph):
        return explore(graph)[0]

    def test_fair_progress_excludes_unfair_starvation(self):
        result = self.report(Graph({0: [('Serve', 2)]}))
        self.assertEqual(result['status'], 'PASS')

    def test_fair_stalled_service_produces_scheduled_lasso(self):
        result = self.report(Graph({0: [('Serve', 0)]}))
        self.assertEqual(result['status'], 'COUNTEREXAMPLE')
        self.assertIn('Serve', [step['action'] for step in result['cycle']])

    def test_deadlock_has_semantic_stuttering_counterexample(self):
        result = self.report(Graph({}))
        self.assertEqual(result['status'], 'COUNTEREXAMPLE')
        self.assertEqual(result['cycle'][0]['action'], 'Observe')

    def test_intermittent_enabling_is_not_strong_fairness(self):
        result = self.report(Graph({0: [('Move', 1), ('Serve', 2)], 1: [('Move', 0)]}))
        self.assertEqual(result['status'], 'COUNTEREXAMPLE')
        self.assertNotIn('Serve', [step['action'] for step in result['cycle']])

    def test_new_arrivals_do_not_invalidate_completed_old_obligation(self):
        result = self.report(Graph({0: [('Serve', 2)], 2: [('NewArrival', 2)]}, pending=lambda s: s == 0))
        self.assertEqual(result['status'], 'PASS')

    def test_reachable_prefix_is_included(self):
        result = self.report(Graph({0: [('Publish', 1)], 1: [('Serve', 1)]}, pending=lambda s: s == 1))
        self.assertEqual(result['trace'][-1]['action'], 'Publish')

    def test_witness_is_identical_across_python_hash_seeds(self):
        code = """import json
from test_explorer import Graph
from explorer import explore
m = Graph({0: [('Serve', 1)], 1: [('Other', 0)]})
m.fair_labels = frozenset({'Serve', 'Other'})
print(json.dumps(explore(m)[0], sort_keys=True))
"""
        results = [subprocess.check_output([sys.executable, '-c', code],
                   cwd=Path(__file__).parent, env=dict(os.environ, PYTHONHASHSEED=str(seed)))
                   for seed in (1, 2, 3)]
        self.assertEqual(len(set(results)), 1)

    def test_state_limit_never_passes(self):
        with self.assertRaises(SearchLimit):
            explore(Graph({0: [('Serve', 2)]}), max_states=1)

    def test_safety_trace_is_shortest(self):
        class Unsafe(Graph):
            def violation(self, before, action, after):
                return 'bad' if after == 2 else None
        result = self.report(Unsafe({0: [('Long', 1), ('Short', 2)], 1: [('End', 2)]}))
        self.assertEqual([s['action'] for s in result['trace']], ['Init', 'Short'])

    def test_scc_witness_connects_distinct_fair_actions(self):
        graph = Graph({0: [('Serve', 1)], 1: [('Other', 0)]})
        graph.fair_labels = frozenset({'Serve', 'Other'})
        result = self.report(graph)
        self.assertEqual(result['status'], 'COUNTEREXAMPLE')
        self.assertEqual(set(result['fairness']), {'Serve', 'Other'})


class LitmusTests(unittest.TestCase):
    def step(self, model, state, action):
        matches = [target for label, target in model.transitions(state) if label == action]
        self.assertEqual(len(matches), 1, action)
        target = matches[0]
        self.assertIsNone(model.violation(state, action, target), action)
        return target

    def recovery_setup(self):
        model, s = Recovery(), Recovery.initial
        for action in ['Acquire:0', 'Fence', 'Confirm', 'Submit:0', 'UploadSegment:0', 'Publish:0']:
            s = self.step(model, s, action)
        return model, s

    def test_cancel_ambiguous_publication_crash_resolves_once(self):
        model, s = Recovery(), Recovery.initial
        for action in ['Acquire:0', 'Fence', 'Confirm', 'Submit:0', 'CancelCaller:0', 'UploadSegment:0',
                       'Publish:0', 'LostResponse:0', 'Crash', 'Reopen', 'Apply', 'Ack:0', 'Replay:0']:
            s = self.step(model, s, action)
        self.assertEqual(s.log, ((0, 1),))
        self.assertEqual(s.applied, (0,))

    def test_lost_response_deferred_apply_handoff_replay(self):
        model, s = self.recovery_setup()
        self.assertEqual(s.applied, ())
        for action in ['LostResponse:0', 'ExpireOwner', 'Acquire:1', 'Fence', 'Confirm',
                       'Apply', 'Ack:0', 'Replay:0']:
            s = self.step(model, s, action)
        self.assertEqual(s.log, ((0, 1),))
        self.assertEqual(s.epoch, 2)

    def test_stale_projection_competing_claims_reserve_disjoint_rows(self):
        model, s = Batching(), Batching.initial
        for action in ['Plan:0', 'Publish:0', 'Respond:0', 'Poll:empty', 'Plan:1', 'Publish:1']:
            s = self.step(model, s, action)
        self.assertEqual(s.applied, 0)
        self.assertEqual(s.selection, ((0,), (1,)))

    def test_renewed_claim_receipt_expires_at_current_deadline(self):
        model, s = Lifecycle(), Lifecycle.initial
        for action in ['Claim', 'Renew:1', 'Tick', 'ReplayClaim:active', 'Tick', 'ReplayClaim:request-expired']:
            s = self.step(model, s, action)
        self.assertEqual(s.expiry, 2)

    def test_old_claim_receipt_cannot_rebind_after_redelivery(self):
        model, s = Lifecycle(), Lifecycle.initial
        for action in ['Claim', 'Tick', 'Claim', 'ReplayOldClaim:1:request-expired']:
            s = self.step(model, s, action)
        self.assertEqual(s.claim_history, (1, 2))

    def test_owner_outage_does_not_pause_eligible_age(self):
        model, s = EligibleAge(), EligibleAge.initial
        for action in ['Tick', 'ToggleOwner', 'Tick', 'PollFilteredEmpty', 'ProjectionLag', 'Tick']:
            s = self.step(model, s, action)
        self.assertEqual(s.age, 2)

    def test_ambiguity_and_shutdown_keep_owned_charge(self):
        model, s = Admission(), Admission.initial
        for action in ['Prepare:0', 'Dispatch:0', 'CancelOwned:0', 'CAS:effect-then-error:0', 'CloseAdmission']:
            s = self.step(model, s, action)
        self.assertEqual(s.charged, (2, 0))
        s = self.step(model, s, 'ResolveAuthority:0')
        self.assertEqual(s.charged, (0, 0))


if __name__ == '__main__':
    unittest.main()
