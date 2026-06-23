#!/usr/bin/env bash
# Delete the Lakebase instance created by provision.sh (instances cost money).
set -euo pipefail
NAME="${PQUEUE_LB_NAME:-pqueue-lakebase-test}"
PROFILE="${DATABRICKS_PROFILE:-dbw-dev-eus2}"
echo ">> deleting Lakebase instance '$NAME'" >&2
databricks --profile "$PROFILE" database delete-database-instance "$NAME" --purge -o json
echo ">> deleted" >&2
