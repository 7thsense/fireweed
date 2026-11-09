"""Large-scale tier of test_coordinator: tests over 10 s, run by the `large` CI job."""
import check_source_group as bootstrap
import json
import unittest
from pathlib import Path
from unittest.mock import patch
import test_coordinator as base
from test_coordinator import gate, model


class LargeCoordinatorTests(unittest.TestCase):
    fail_closed = base.CoordinatorTests.fail_closed
    def test_ninth_identity_search_and_runtime_are_mandatory(self):
        self.fail_closed(lambda:patch.object(model.Coordinator,'name','omitted-coordinator'),'ninth exact source coordinator identity required')
        original=gate.helper.explore
        def limited(spec,cap):return original(spec,1 if spec.name==model.Coordinator.name and spec.fault is None else cap)
        self.fail_closed(lambda:patch.object(gate.helper,'explore',limited),'api007-native-commit-coordinator: incomplete search at 1 states')
        def broken(spec,cap):
            if spec.name==model.Coordinator.name:raise RuntimeError('injected coordinator runtime')
            return original(spec,cap)
        self.fail_closed(lambda:patch.object(gate.helper,'explore',broken),'injected coordinator runtime')


if __name__ == "__main__":
    unittest.main()
