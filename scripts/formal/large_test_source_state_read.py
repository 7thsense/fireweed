"""Large-scale tier of test_source_state_read: tests over 10 s, run by the `large` CI job."""
import unittest
from unittest.mock import patch
import test_source_state_read as base
from test_source_state_read import gate, model, oracle


class LargeSourceStateReadTests(unittest.TestCase):
    setUpClass = classmethod(base.SourceStateReadTests.setUpClass.__func__)
    def test_missing_campaign_witness_or_control_blocks_aggregate_gate(self):
        variants=[({**self.report,'name':'other'},self.controls),
                  ({**self.report,'complete':False},self.controls),
                  ({**self.report,'completed_witnesses':{}},self.controls),
                  (self.report,[])]
        for result in variants:
            with patch.object(oracle,'run_campaign',lambda *args,result=result:result):
                with self.assertRaises(AssertionError):gate.run()


if __name__ == "__main__":
    unittest.main()
