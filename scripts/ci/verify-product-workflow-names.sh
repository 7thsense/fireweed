#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: verify-product-workflow-names.sh <suite-list.toml>" >&2
    exit 2
fi

python3 - "$1" <<'PY'
import sys, tomllib
required={
 "product_validation_tests",
 "product_workflow_scheduled_action_delivery_e2e",
 "product_workflow_marketo_group_batching_e2e",
 "product_workflow_callback_cohort_e2e",
 "product_workflow_jobs_connectors_recurring_e2e",
 "product_workflow_worker_crash_recovery_e2e",
 "product_workflow_noisy_neighbor_scale_e2e",
 "product_workflow_generic_priority_bounded_relaxed_e2e",
 "product_workflow_downstream_pacing_non_goal_e2e",
}
data=tomllib.load(open(sys.argv[1],"rb"))
names={s.get("name") for s in data.get("suites",[])}
missing=required-names
if missing:
    print("missing suite names: " + ", ".join(sorted(missing)), file=sys.stderr)
    sys.exit(1)
print("product workflow suite names verified")
PY
