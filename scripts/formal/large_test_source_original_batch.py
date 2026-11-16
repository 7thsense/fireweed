"""Large-scale tier of test_source_original_batch: tests over 10 s, run by the `large` CI job."""
import check_source_group as bootstrap
import json
import unittest
from pathlib import Path
from unittest.mock import patch
import test_source_original_batch as base
from test_source_original_batch import gate


class LargeSourceOriginalBatchTests(unittest.TestCase):
    fail_closed = base.SourceOriginalBatchTests.fail_closed
    def test_eighth_search_limit_and_runtime_error_invalidate_stale_pass(self):
        original=gate.helper.explore
        def limited(spec,cap):
            return original(spec,1 if spec.name=='api007-source-original-batch-resolution' and spec.fault is None else cap)
        self.fail_closed(lambda:patch.object(gate.helper,'explore',limited),
            'api007-source-original-batch-resolution: incomplete search at 1 states')
        def broken(spec,cap):
            if spec.name=='api007-source-original-batch-resolution':raise RuntimeError('injected original batch exploration runtime')
            return original(spec,cap)
        self.fail_closed(lambda:patch.object(gate.helper,'explore',broken),'injected original batch exploration runtime')


if __name__ == "__main__":
    unittest.main()
