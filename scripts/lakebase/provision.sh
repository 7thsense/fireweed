#!/usr/bin/env bash
# Provision a Databricks Lakebase instance for pqueue live acceptance tests.
#
# Requires an authenticated Databricks CLI:
#   databricks auth login --profile "$DATABRICKS_PROFILE"
#
# Usage:
#   scripts/lakebase/provision.sh            # create + print connection info
#   PQUEUE_LB_NAME=my-instance scripts/lakebase/provision.sh
#
# Then export the printed PQUEUE_LAKEBASE_DSN and run:
#   cargo test -p pqueue-postgres --features tls --test lakebase_live_tests -- --ignored --nocapture
#
# Tear down with scripts/lakebase/teardown.sh when done (instances cost money).
set -euo pipefail

NAME="${PQUEUE_LB_NAME:-pqueue-lakebase-test}"
CAPACITY="${PQUEUE_LB_CAPACITY:-CU_1}"
PROFILE="${DATABRICKS_PROFILE:-dbw-dev-eus2}"
DB="${PQUEUE_LB_DBNAME:-databricks_postgres}"

db() { databricks --profile "$PROFILE" "$@"; }

echo ">> verifying Databricks auth ($PROFILE)" >&2
if ! db database list-database-instances -o json >/dev/null 2>&1; then
  echo "!! Databricks CLI not authenticated. Run: databricks auth login --profile $PROFILE" >&2
  exit 1
fi

echo ">> creating Lakebase instance '$NAME' (capacity=$CAPACITY, pg-native-login on)" >&2
db database create-database-instance "$NAME" \
  --capacity "$CAPACITY" \
  --enable-pg-native-login \
  --node-count 1 \
  -o json >/tmp/pqueue-lb-create.json || {
    echo ">> create failed or instance exists; fetching existing" >&2
  }

echo ">> waiting for AVAILABLE state" >&2
HOST=""
for _ in $(seq 1 60); do
  INFO=$(db database get-database-instance "$NAME" -o json 2>/dev/null || true)
  STATE=$(printf '%s' "$INFO" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("state",""))' 2>/dev/null || true)
  HOST=$(printf '%s' "$INFO" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("read_write_dns",""))' 2>/dev/null || true)
  echo "   state=$STATE host=$HOST" >&2
  [ "$STATE" = "AVAILABLE" ] && [ -n "$HOST" ] && break
  sleep 10
done
[ -n "$HOST" ] || { echo "!! instance did not become AVAILABLE with a host" >&2; exit 1; }

# OAuth-direct credential (token-as-password). Identity = current CLI principal.
REQ_ID=$(python3 -c 'import uuid;print(uuid.uuid4())')
CRED=$(db database generate-database-credential --request-id "$REQ_ID" \
  --json "{\"instance_names\":[\"$NAME\"]}" -o json)
TOKEN=$(printf '%s' "$CRED" | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')
USER=$(db current-user me -o json 2>/dev/null | python3 -c 'import sys,json;print(json.load(sys.stdin).get("userName",""))' || true)

echo "" >&2
echo ">> instance ready. OAuth-direct DSN (token expires ~60 min):" >&2
echo "export PQUEUE_LAKEBASE_DSN=\"host=$HOST port=5432 user=$USER password=$TOKEN dbname=$DB sslmode=require\""
echo "" >&2
echo ">> for a stable native-password DSN, create a Postgres role + password on the" >&2
echo "   instance and use that instead of the OAuth token." >&2
