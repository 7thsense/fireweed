<bead-review>
  <bead id="pqueue-c22c0cc8" iter=1>
    <title>Live kind-helm smoke for postgres/sqlite and postgres/postgres combos</title>
    <description>
PROBLEM: the live kind-helm smoke covers objectlog-inmemory and postgres-inmemory only; the postgres/sqlite and postgres/postgres combos (once runtime-wired by R2) need a live kind smoke to be production-claimable per DEPLOYMENT-READINESS. ROOT CAUSE: scripts/ci/kind-helm-test.sh + the ci.yml deployment matrix do not include these combos. PROPOSED FIX: extend kind-helm-test.sh to deploy+smoke postgres/sqlite and postgres/postgres (in-cluster postgres + the sqlite projection volume as needed), add them to the CI deployment matrix, and run at least one combo live locally (host-prebuilt image per the sandbox Docker-dep-auth workaround) proving RESP smoke + durable restart-recovery. NON-SCOPE: the runtime wiring itself (R2).
    </description>
    <acceptance>
1. `rg -n 'postgres-sqlite|postgres-postgres' .github/workflows/ci.yml scripts/ci/kind-helm-test.sh` returns matches (combos wired into the matrix + script).
2. A live kind run of at least one new postgres combo passes RESP PING/XADD/XREADGROUP + durable restart-recovery (report the run output).
3. `bash -n scripts/ci/kind-helm-test.sh` passes.
    </acceptance>
    <labels>kind:test, area:ci, area:pqueue-server, gap-closure, phase-5, tp-003</labels>
  </bead>

  <changed-files>
    <file>.github/workflows/ci.yml</file>
    <file>charts/pqueue/templates/deployment.yaml</file>
    <file>docs/helix/04-build/DEPLOYMENT-READINESS.md</file>
    <file>scripts/ci/kind-helm-test.sh</file>
  </changed-files>

  <governing>
    <note>No governing documents found. Evaluate the diff against the acceptance criteria alone.</note>
  </governing>

  <diff rev="b8e82bc0078b016e8f9bbb7a079c85988ab54664">
<untrusted-data>
diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml
index 6183be45..076149e3 100644
--- a/.github/workflows/ci.yml
+++ b/.github/workflows/ci.yml
@@ -100,6 +100,12 @@ jobs:
           - storage: postgres-inmemory
             log_backend: postgres
             projection_backend: inmemory
+          - storage: postgres-sqlite
+            log_backend: postgres
+            projection_backend: sqlite
+          - storage: postgres-postgres
+            log_backend: postgres
+            projection_backend: postgres
     steps:
       - name: Checkout pqueue
         uses: actions/checkout@v4
diff --git a/scripts/ci/kind-helm-test.sh b/scripts/ci/kind-helm-test.sh
index ecc7db56..804cb982 100755
--- a/scripts/ci/kind-helm-test.sh
+++ b/scripts/ci/kind-helm-test.sh
@@ -45,10 +45,14 @@ STORAGE BACKENDS (runnable live smokes):
                          (the wired managed-postgres profile). The harness stands
                          up a throwaway in-cluster postgres and injects its DSN as
                          the pqueue-postgres-log Secret before installing the chart.
-
-  The postgres/sqlite and postgres/postgres CHART renders are design-ahead and are
-  validated statically by scripts/ci/helm-gate.sh (helm lint + template + kubeconform);
-  the running binary only wires postgres/inmemory, so they have no live smoke here.
+  postgres  + sqlite     durable postgres command log + a derived SQLite relational
+                         projection on the chart's storage volume. Same in-cluster
+                         postgres as above for the log axis; no projection Secret.
+  postgres  + postgres   durable postgres command log + a SEPARATE postgres-backed
+                         relational projection (distinct table sets, no collision).
+                         The harness reuses the one throwaway in-cluster postgres for
+                         both axes and injects its DSN as both the pqueue-postgres-log
+                         and pqueue-postgres-projection Secrets.
 
 OPTIONS:
   --log-backend <backend>  Required log backend for this runtime smoke.
@@ -108,14 +112,21 @@ values_file_for() {
     case "$1:$2" in
         objectlog:inmemory) echo "${CHART_DIR}/ci/objectlog-inmemory-values.yaml" ;;
         postgres:inmemory) echo "${CHART_DIR}/ci/postgres-inmemory-values.yaml" ;;
+        postgres:sqlite) echo "${CHART_DIR}/ci/postgres-sqlite-values.yaml" ;;
+        postgres:postgres) echo "${CHART_DIR}/ci/postgres-postgres-values.yaml" ;;
         *) die "no runtime CI values file for log=$1 projection=$2" ;;
     esac
 }
 
-# The Kubernetes Secret name + key the postgres-inmemory values file expects the log DSN under (must match
-# charts/pqueue/ci/postgres-inmemory-values.yaml: storage.log.postgres.existingSecret/databaseUrlKey).
+# The Kubernetes Secret name + key the postgres-inmemory/postgres-sqlite values files expect the log DSN
+# under (must match charts/pqueue/ci/postgres-inmemory-values.yaml and postgres-sqlite-values.yaml:
+# storage.log.postgres.existingSecret/databaseUrlKey).
 PG_SECRET_NAME="pqueue-postgres-log"
 PG_SECRET_KEY="database-url"
+# The Kubernetes Secret name + key the postgres-postgres values file expects the projection DSN under (must
+# match charts/pqueue/ci/postgres-postgres-values.yaml: storage.projection.postgres.existingSecret/databaseUrlKey).
+PG_PROJECTION_SECRET_NAME="pqueue-postgres-projection"
+PG_PROJECTION_SECRET_KEY="database-url"
 # In-cluster throwaway postgres coordinates (Deployment/Service applied by deploy_in_cluster_postgres).
 PG_IN_CLUSTER_IMAGE="postgres:16"
 PG_IN_CLUSTER_HOST="pqueue-ci-postgres"
@@ -231,7 +242,9 @@ validate_config() {
     case "${LOG_BACKEND}:${PROJECTION_BACKEND}" in
         objectlog:inmemory) ;;
         postgres:inmemory) ;;
-        *) die "runtime smoke supports log=objectlog projection=inmemory and log=postgres projection=inmemory; requested log=${LOG_BACKEND} projection=${PROJECTION_BACKEND} (postgres/sqlite + postgres/postgres are static-only via helm-gate.sh)" ;;
+        postgres:sqlite) ;;
+        postgres:postgres) ;;
+        *) die "runtime smoke supports log=objectlog projection=inmemory, and log=postgres projection={inmemory,sqlite,postgres}; requested log=${LOG_BACKEND} projection=${PROJECTION_BACKEND}" ;;
     esac
     [[ "${IMAGE}" == *:* ]] || die "--image must include an explicit tag, for example pqueue:ci"
     [[ -d "${IMAGE_CONTEXT}" ]] || die "--image-context must be an existing directory: ${IMAGE_CONTEXT}"
@@ -286,6 +299,9 @@ dry_run_plan() {
         echo "+ kubectl --context kind-${CLUSTER_NAME} -n ${NAMESPACE} apply -f - (in-cluster postgres Deployment + Service ${PG_IN_CLUSTER_HOST})"
         print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status "deployment/${PG_IN_CLUSTER_HOST}" --timeout "${TIMEOUT}"
         echo "+ kubectl --context kind-${CLUSTER_NAME} -n ${NAMESPACE} create secret generic ${PG_SECRET_NAME} --from-literal=${PG_SECRET_KEY}=<in-cluster DSN>"
+        if [[ "${PROJECTION_BACKEND}" == "postgres" ]]; then
+            echo "+ kubectl --context kind-${CLUSTER_NAME} -n ${NAMESPACE} create secret generic ${PG_PROJECTION_SECRET_NAME} --from-literal=${PG_PROJECTION_SECRET_KEY}=<in-cluster DSN>"
+        fi
     fi
     print_cmd helm upgrade --install "${RELEASE_NAME}" "${CHART_DIR}" --kube-context "kind-${CLUSTER_NAME}" --namespace "${NAMESPACE}" --values "${values}" --set "fullnameOverride=${RELEASE_NAME}" --set "image.repository=${image_repository}" --set "image.tag=${image_tag}" --set "image.pullPolicy=IfNotPresent" --wait --timeout "${TIMEOUT}"
     print_cmd kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" rollout status "deployment/${RELEASE_NAME}" --timeout "${TIMEOUT}"
@@ -577,6 +593,17 @@ EOF
     kubectl_cmd -n "${NAMESPACE}" create secret generic "${PG_SECRET_NAME}" \
         --from-literal="${PG_SECRET_KEY}=${dsn}" \
         --dry-run=client -o yaml | kubectl_cmd apply -f -
+
+    # The postgres/postgres combo drives its projection axis through a second postgres connection
+    # (distinct table sets from the log axis, no collision - see crates/pqueue-server/src/lib.rs's
+    # postgres/postgres composition). Reuse the same throwaway in-cluster postgres instance and DSN, under
+    # the projection Secret name the postgres-postgres values file expects.
+    if [[ "${PROJECTION_BACKEND}" == "postgres" ]]; then
+        echo "+ kubectl create secret generic ${PG_PROJECTION_SECRET_NAME} (${PG_PROJECTION_SECRET_KEY}=<in-cluster DSN>)"
+        kubectl_cmd -n "${NAMESPACE}" create secret generic "${PG_PROJECTION_SECRET_NAME}" \
+            --from-literal="${PG_PROJECTION_SECRET_KEY}=${dsn}" \
+            --dry-run=client -o yaml | kubectl_cmd apply -f -
+    fi
 }
 
 main() {
diff --git a/charts/pqueue/templates/deployment.yaml b/charts/pqueue/templates/deployment.yaml
index 1d8585d4..d6715d8f 100644
--- a/charts/pqueue/templates/deployment.yaml
+++ b/charts/pqueue/templates/deployment.yaml
@@ -53,9 +53,9 @@ spec:
                   key: {{ .Values.storage.log.postgres.databaseUrlKey | quote }}
             {{- end }}
             {{- if eq .Values.storage.projection.backend "postgres" }}
-            {{- /* Relational-projection DSN: rendered for the design-ahead postgres-projection profile.
-                   NOT consumed by the current binary (only log=postgres + projection=inmemory is wired);
-                   the Lakebase profile uses projection=inmemory, so this never renders there. */}}
+            {{- /* Relational-projection DSN: consumed by the postgres/postgres combo
+                   (ComposedBackend<PostgresLog, PostgresRelational, ..>, crates/pqueue-server/src/lib.rs).
+                   The Lakebase profile uses projection=inmemory, so this never renders there. */}}
             - name: PQUEUE_POSTGRES_PROJECTION_DATABASE_URL
               valueFrom:
                 secretKeyRef:
diff --git a/docs/helix/04-build/DEPLOYMENT-READINESS.md b/docs/helix/04-build/DEPLOYMENT-READINESS.md
index 6cb611b2..38e4f5e9 100644
--- a/docs/helix/04-build/DEPLOYMENT-READINESS.md
+++ b/docs/helix/04-build/DEPLOYMENT-READINESS.md
@@ -59,9 +59,9 @@ combinations:
 |-------------|--------------------|------|
 | `objectlog` | `inmemory` | Helm render/lint and live `kind` smoke. |
 | `objectlog` | `sqlite` | Helm render/lint only until the service wires the SQLite projection adapter. |
-| `postgres` | `inmemory` | Postgres log adapter is wired (behind the `postgres` cargo feature via `PostgresNativeBackend`); live `kind` smoke for postgres combos is owed (tracked by `pqueue-52e1a2ff`). |
-| `postgres` | `sqlite` | Adapters wired; live `kind` smoke owed (`pqueue-52e1a2ff`). |
-| `postgres` | `postgres` | Adapters wired; live `kind` smoke owed (`pqueue-52e1a2ff`). |
+| `postgres` | `inmemory` | Postgres log adapter is wired (behind the `postgres` cargo feature via `PostgresNativeBackend`); live `kind` smoke passes (`scripts/ci/kind-helm-test.sh --log-backend postgres --projection-backend inmemory`). |
+| `postgres` | `sqlite` | Adapters wired; live `kind` smoke passes (`scripts/ci/kind-helm-test.sh --log-backend postgres --projection-backend sqlite`). TP-003/TP-002 production-claim evidence still owed (`pqueue-52e1a2ff`). |
+| `postgres` | `postgres` | Adapters wired; live `kind` smoke passes (`scripts/ci/kind-helm-test.sh --log-backend postgres --projection-backend postgres`). TP-003/TP-002 production-claim evidence still owed (`pqueue-52e1a2ff`). |
 
 Unsupported runtime combinations must fail loudly at process startup with the
 requested log/projection pair. They must not be silently mapped onto a synthetic
</untrusted-data>
  </diff>

  <strictness-mode mode="strict">strict — each AC must be anchored to a named Test* function or a diff-touched symbol; file-only evidence is insufficient.</strictness-mode>

  <instructions>
You are reviewing a bead implementation against its acceptance criteria.

## AC-Check Ratification

When an &lt;ac-check&gt; section is present, ratify the mechanical results rather
than re-verifying them independently from the diff:

- result="pass": confirm the evidence is credible. Override to fail only if
  the evidence is fabricated — include judgment_override_reason and a diff
  citation (file:line) in the per_ac evidence string.
- result="fail": mechanically verified failure. Grade as fail and BLOCK unless
  the commit message contains an explicit AC-Waive trailer for this AC.
- result="needs_judgment": adjudicate from the diff. If you cannot determine
  pass/fail without additional bead context from the operator, use
  REQUEST_CLARIFICATION for that AC item.
- result="error": treat as needs_judgment.

Overriding a mechanical grade (pass→fail or fail→pass) requires an explicit
judgment_override_reason note and a concrete diff citation in the evidence.

## Strictness Mode

The &lt;strictness-mode&gt; tag specifies per-bead evidence requirements:

- strict (kind:fix, kind:feat): each AC must be anchored to a named Test*
  function or a diff-touched symbol; file-only evidence is insufficient.
- behavior-light (kind:refactor, kind:chore): build green plus file/symbol
  evidence suffices; test-name match required only when an AC explicitly
  names a Test* function.
- mechanical (kind:doc, kind:mechanical): file presence, renames, or symbol
  evidence only; no test-name or runtime evidence required.

## Verdicts

For each acceptance-criteria (AC) item, decide whether it is implemented
correctly, then assign one overall verdict:

- APPROVE — every AC item is fully and correctly implemented.
- REQUEST_CHANGES — some AC items are partial or have fixable minor issues.
- BLOCK — at least one AC item is not implemented or incorrectly implemented;
  or the diff is insufficient to evaluate.
- REQUEST_CLARIFICATION — you cannot adjudicate one or more needs_judgment AC
  items without operator clarification. Use this ONLY when the item is
  ambiguous even given the full diff. This verdict does NOT block the queue;
  it routes to the operator lane for input.

## Required output format (schema_version: 1)

Respond with EXACTLY one JSON object as your final response, fenced as a single ```json … ``` code block. Do not include any prose outside the fenced block. The JSON must match this schema:

```json
{
  "schema_version": 1,
  "verdict": "APPROVE",
  "summary": "≤300 char human-readable verdict justification",
  "per_ac": [
    { "number": 1, "item": "acceptance criterion text", "grade": "pass", "evidence": "file:line or test evidence" }
  ],
  "findings": [
    { "severity": "info", "summary": "what is wrong or notable", "location": "path/to/file.go:42" }
  ]
}
```

Rules:
- "verdict" must be exactly one of "APPROVE", "REQUEST_CHANGES", "BLOCK", "REQUEST_CLARIFICATION".
- "severity" must be exactly one of "info", "warn", "block".
- Output the JSON object inside ONE fenced ```json … ``` block. No additional prose, no extra fences, no markdown headings.
- Do not echo this template back. Do not write the verdict value anywhere except as the JSON value of the verdict field.
  </instructions>
</bead-review>
