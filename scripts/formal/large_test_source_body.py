"""Large-scale tier of test_source_body: tests over 10 s, run by the `large` CI job."""
import check_source_group as bootstrap
import json
import unittest
from pathlib import Path
from unittest.mock import patch
import test_source_body as base
from test_source_body import gate, helper, model, oracle


class LargeSourceBodyTests(unittest.TestCase):
    fail_closed = base.SourceBodyTests.fail_closed
    def test_missing_witness_mutant_and_nonpassing_campaign_fail_closed(self):
        original=oracle.run_campaign
        for mode,message in [('witness','all body completion witnesses required'),('mutant','all body mutant controls required'),('status','complete passing body campaign required')]:
            def damaged(h,m,cap,mode=mode):
                r,c=original(h,m,cap)
                if mode=='witness':r['completed_witnesses'].pop('metadata_only_unreadable_body')
                elif mode=='mutant':c=c[:-1]
                else:r['status']='INCOMPLETE'
                return r,c
            self.fail_closed(lambda:patch.object(oracle,'run_campaign',damaged),message)
    def test_tenth_identity_and_complete_bound_are_mandatory(self):
        self.fail_closed(lambda:patch.object(model.SourceBodies,'name','omitted-body'),'tenth exact source body identity required')
        original=helper.explore
        def limited(spec,cap):return original(spec,1 if spec.name==model.SourceBodies.name and spec.fault is None else cap)
        self.fail_closed(lambda:patch.object(helper,'explore',limited),'api007-addressed-body-materialization: incomplete search at 1 states')


if __name__ == "__main__":
    unittest.main()
