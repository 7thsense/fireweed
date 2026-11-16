"""Large-scale tier of test_source_selection: tests over 10 s, run by the `large` CI job."""
import unittest
from unittest.mock import patch
import test_source_selection as base
from test_source_selection import gate, model, oracle


class LargeSourceSelectionTests(unittest.TestCase):
    setUpClass = classmethod(base.SourceSelectionTests.setUpClass.__func__)
    def test_missing_or_incomplete_campaign_blocks_aggregate(self):
        for result in [({**self.report,'name':'other'},self.controls),
                       ({**self.report,'complete':False},self.controls),
                       ({**self.report,'completed_witnesses':{}},self.controls),
                       (self.report,[])]:
            with patch.object(oracle,'run_campaign',lambda *args,result=result:result):
                with self.assertRaises(AssertionError):gate.run()


if __name__ == "__main__":
    unittest.main()
