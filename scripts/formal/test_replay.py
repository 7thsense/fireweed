"""Controls for complete deterministic generation and independent predictions."""
import copy
import json
import unittest
from generate_replay import REQUIRED, generate, graph, validate_corpus, scheduled_edges


class ReplayGenerationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.corpus = generate()

    def test_complete_deterministic_graph_and_required_compositions(self):
        self.assertEqual(self.corpus, generate())
        states, _, _, edges = graph()
        self.assertEqual(self.corpus['graph'], {'complete': True, 'states': len(states), 'edges': len(edges)})
        sequences = [[step['action'] for step in trace['steps']] for trace in self.corpus['traces']]
        for sequence in REQUIRED:
            self.assertIn(sequence, sequences)
        self.assertTrue(validate_corpus(self.corpus))

    def test_full_edge_selection_and_generation_cap(self):
        corpus = generate('edges')
        self.assertEqual(corpus['selection']['intended_edges'], corpus['graph']['edges'])
        self.assertTrue(validate_corpus(corpus))
        with self.assertRaises(ValueError):
            generate(limit=10)

    def test_concurrency_continuation_preserves_both_branches(self):
        trace = next(t for t in self.corpus['traces'] if [s['action'] for s in t['steps']][:2] == ['ConcurrentClaims', 'LoseResponse:0'])
        first, second = trace['steps'][:2]
        self.assertEqual(len(first['transitions']), 2)
        self.assertEqual({e['after'] for e in first['transitions']}, {e['before'] for e in second['transitions']})

    def test_corrupt_schema_observation_transitions_and_coverage_rejected(self):
        modifications = [
            lambda c: c.update(schema='bad'),
            lambda c: c.update(model_sha256='0'*64),
            lambda c: c.update(traces=[]),
            lambda c: c['graph'].update(complete=False),
            lambda c: c['graph'].update(edges=1),
            lambda c: c['selection'].update(intended_edges=0),
            lambda c: c.update(fingerprint_body='wrong'),
            lambda c: c['traces'][0]['steps'][0].update(action='Unknown'),
            lambda c: c['traces'][0]['steps'][0].update(transitions=[]),
            lambda c: c['states']['0']['observation'].update(owner=1),
            lambda c: c['traces'][0].update(intended_edges=[]),
        ]
        for modify in modifications:
            corpus = copy.deepcopy(self.corpus)
            modify(corpus)
            with self.assertRaises((ValueError, AssertionError, KeyError, TypeError)):
                validate_corpus(corpus)

    def test_checked_in_fixture_is_canonical(self):
        from pathlib import Path
        fixture = json.loads(Path(__file__).with_name('fixtures').joinpath('replay-smoke.json').read_text())
        self.assertEqual(fixture, json.loads(json.dumps(self.corpus)))


class ScheduledCoverageTests(unittest.TestCase):
    def test_schedule_expansion_covers_exact_graph_with_unchanged_oracle(self):
        corpus = generate('exhaustive')
        self.assertEqual(len(corpus['traces']), 20920)
        self.assertEqual(sum(len(t['steps']) for t in corpus['traces']), 153819)
        actual = set().union(*(scheduled_edges(t, t['schedule']) for t in corpus['traces']))
        graph_edges = {f'{a}|{b}|{c}' for a,b,c,_ in graph()[3]}
        self.assertEqual(actual, graph_edges)
        forward = [t for t in corpus['traces'] if t['schedule']=='request-0-first']
        self.assertEqual(len(set().union(*(scheduled_edges(t, t['schedule']) for t in forward))),16995)
        reverse = next(t for t in corpus['traces'] if t['schedule']=='request-1-first')
        self.assertTrue(any(len(s['transitions'])==2 for s in reverse['steps'] if s['action']=='ConcurrentClaims'))
        self.assertTrue(validate_corpus(corpus))

    def test_missing_or_corrupted_schedule_and_dropped_cases_fail_closed(self):
        corpus = generate('schedule-smoke')
        for kind in ('missing','unknown','drop'):
            corrupt = copy.deepcopy(corpus)
            if kind=='missing': del corrupt['traces'][0]['schedule']
            elif kind=='unknown': corrupt['traces'][0]['schedule']='invented'
            else: corrupt['traces'].pop(); corrupt['selection']['traces']-=1
            with self.assertRaises(ValueError): validate_corpus(corrupt)

    def test_forged_coverage_or_unrealized_schedule_fails_closed(self):
        from run_replay import reconcile_witnessed
        corpus = generate('schedule-smoke')
        cases = [{'steps':[{'matched_edges':sorted(scheduled_edges(t,t['schedule']))}]} for t in corpus['traces']]
        union = sorted(set().union(*(scheduled_edges(t,t['schedule']) for t in corpus['traces'])))
        detail = {'cases':cases,'matched_edges':union,'sut_graph_edge_exhaustive':False}
        self.assertEqual(reconcile_witnessed(corpus,detail),set(union))
        for kind in ('aggregate','exhaustive','schedule'):
            wrong = copy.deepcopy(detail)
            if kind=='aggregate': wrong['matched_edges']=union+['0|fake|1']
            elif kind=='exhaustive': wrong['sut_graph_edge_exhaustive']=True
            else:
                i=next(i for i,t in enumerate(corpus['traces']) if t['schedule']=='request-1-first')
                wrong['cases'][i]['steps'][0]['matched_edges']=sorted(scheduled_edges(corpus['traces'][i],'request-0-first'))
            with self.assertRaises(ValueError): reconcile_witnessed(corpus,wrong)


class FixtureCapacityTests(unittest.TestCase):
    def test_inode_exhaustion_rejected_despite_free_bytes(self):
        from with_rustfs import require_fixture_capacity
        limited = {'inode_total':1048576,'inode_available':604289,'bytes_available':40*1024**3}
        with self.assertRaisesRegex(RuntimeError,'inode'): require_fixture_capacity(limited,20920)
        disk = dict(limited,inode_total=781352128,inode_available=779886275)
        self.assertEqual(require_fixture_capacity(disk,20920)['inodes'],1347072)
        self.assertEqual(require_fixture_capacity(dict(disk,inode_total=0,inode_available=0),20920)['expected_cases'],20920)

    def test_byte_exhaustion_is_independently_rejected(self):
        from with_rustfs import require_fixture_capacity
        with self.assertRaisesRegex(RuntimeError,'byte'):
            require_fixture_capacity({'inode_total':0,'inode_available':0,'bytes_available':1},96)
