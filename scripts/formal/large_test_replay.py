"""Large-scale tier of test_replay: tests over 10 s, run by the `large` CI job."""
import unittest
from generate_replay import generate, graph, scheduled_edges, validate_corpus


class LargeScheduledCoverageTests(unittest.TestCase):
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


class LargeReplayGenerationTests(unittest.TestCase):
    def test_full_edge_selection_and_generation_cap(self):
        corpus = generate('edges')
        self.assertEqual(corpus['selection']['intended_edges'], corpus['graph']['edges'])
        self.assertTrue(validate_corpus(corpus))
        with self.assertRaises(ValueError):
            generate(limit=10)


if __name__ == "__main__":
    unittest.main()
