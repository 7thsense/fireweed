"""Large-scale tier of test_source_batch: tests over 10 s, run by the `large` CI job."""
import check_source_group as bootstrap
import json
import unittest
from pathlib import Path
from unittest.mock import patch
import test_source_batch as base
from test_source_batch import gate


class LargeSourceBatchTests(unittest.TestCase):
    fail_closed = base.SourceBatchTests.fail_closed
    def test_only_seventh_search_limit_and_runtime_error_replace_stale_pass(self):
        original=gate.helper.explore
        def limited(spec,cap):
            return original(spec,1 if spec.name=='api007-source-batch-composition' and spec.fault is None else cap)
        self.fail_closed(lambda:patch.object(gate.helper,'explore',limited),
            'api007-source-batch-composition: incomplete search at 1 states')
        def broken(spec,cap):
            if spec.name=='api007-source-batch-composition':raise RuntimeError('injected batch exploration runtime')
            return original(spec,cap)
        self.fail_closed(lambda:patch.object(gate.helper,'explore',broken),'injected batch exploration runtime')


if __name__ == "__main__":
    unittest.main()
