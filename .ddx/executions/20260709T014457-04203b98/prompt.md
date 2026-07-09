<bead-review>
  <bead id="pqueue-86c8b0ee" iter=1>
    <title>Runtime-wire postgres/sqlite and postgres/postgres storage combos</title>
    <description>
PROBLEM: the server runtime (crates/pqueue-server/src/env_config.rs) only wires the postgres/inmemory storage combo; postgres-log x sqlite-projection and postgres-log x postgres-projection are static-Helm-render-only, so per the DEPLOYMENT-READINESS contract they are not production-claimable. ROOT CAUSE: env_config's backend selector has no arm building the composed postgres-log x {sqlite,postgres} projection backends under the tokio/spawn_blocking wiring. PROPOSED FIX: wire both combos in the runtime selector using the composed backend constructors + the BlockingBackend/spawn_blocking pattern (from B3.4), driving them under an ambient tokio runtime without the sync-client panic; a failed/unsupported combo still fails loudly at startup. NON-SCOPE: kind smoke (separate bead R3); AC-TXN matrices.
    </description>
    <acceptance>
1. `postgres_sqlite_combo_runs_under_tokio` passes (push/claim/finalize over the composed postgres-log + sqlite-projection backend under a tokio runtime, env-gated on PQUEUE_PG_TEST_URL).
2. `postgres_postgres_combo_runs_under_tokio` passes similarly for postgres-log + postgres-projection.
3. `rg -n 'postgres.*sqlite|postgres.*postgres' crates/pqueue-server/src/env_config.rs` shows both combos wired in the runtime selector.
4. `rustup run 1.92.0 cargo test -p pqueue-server --features postgres` passes.
    </acceptance>
    <labels>kind:build, area:pqueue-server, area:pqueue-postgres, gap-closure, phase-5, tp-003</labels>
  </bead>

  <changed-files>
    <file>.ddx/executions/20260709T012941-11bad071/report.md</file>
    <file>crates/pqueue-server/src/env_config.rs</file>
    <file>crates/pqueue-server/src/lib.rs</file>
    <file>crates/pqueue-server/tests/postgres_composed_projections.rs</file>
  </changed-files>

  <governing>
    <note>No governing documents found. Evaluate the diff against the acceptance criteria alone.</note>
  </governing>

  <diff rev="19be53b22bafd7c4690a0e88bcfd6e4e85b93d7b">
<untrusted-data>
diff --git a/crates/pqueue-server/src/env_config.rs b/crates/pqueue-server/src/env_config.rs
index efc21c48..54108bf7 100644
--- a/crates/pqueue-server/src/env_config.rs
+++ b/crates/pqueue-server/src/env_config.rs
@@ -265,19 +265,38 @@ fn parse_backend(env: &BTreeMap<String, String>) -> Result<BackendSpec, ConfigEr
                 "/var/lib/pqueue/pqueue-projection.db",
             )),
         },
+        #[cfg(feature = "postgres")]
+        "postgres" => {
+            // Resolve the DSN from the env names the Helm chart's `storage.projection.postgres` axis
+            // renders (DSN secret `PQUEUE_POSTGRES_PROJECTION_DATABASE_URL`; `PQUEUE_PG_PROJECTION_URL` is
+            // the local/dev fallback). Fails closed if an sslmode=require DSN meets a non-tls build.
+            crate::resolve_postgres_projection(env)
+                .map_err(|reason| unsupported_storage(&log, &projection, &reason))?
+        }
+        #[cfg(not(feature = "postgres"))]
+        "postgres" => {
+            return Err(unsupported_storage(
+                &log,
+                &projection,
+                "postgres projection adapter is wired through the blocking-safe PostgresRelational store, \
+                 but this binary was built without the `postgres` cargo feature; rebuild with `--features \
+                 postgres` (or `--features postgres,tls` for native-tls)",
+            ));
+        }
         other => {
             return Err(unsupported_storage(
                 &log,
                 &projection,
                 &format!(
-                    "unknown PQUEUE_PROJECTION_BACKEND={other:?}; expected inmemory|sqlite|hybrid|hybrid-async"
+                    "unknown PQUEUE_PROJECTION_BACKEND={other:?}; expected inmemory|sqlite|hybrid|hybrid-async|postgres"
                 ),
             ));
         }
     };
 
     // Only specific log×projection pairings are wired (preserve the prior behavior): memory/inmemory,
-    // sqlite/inmemory, objectlog/inmemory, objectlog/sqlite, and (with the feature) postgres/inmemory.
+    // sqlite/inmemory, objectlog/inmemory, objectlog/sqlite, and (with the feature) postgres/inmemory,
+    // postgres/sqlite, postgres/postgres.
     let wired = match (&log_spec, &projection_spec) {
         (LogSpec::Memory, ProjectionSpec::InMemory) => true,
         (LogSpec::Sqlite { .. }, ProjectionSpec::InMemory) => true,
@@ -287,6 +306,10 @@ fn parse_backend(env: &BTreeMap<String, String>) -> Result<BackendSpec, ConfigEr
         (LogSpec::ObjectLog { .. }, ProjectionSpec::HybridAsync { .. }) => true,
         #[cfg(feature = "postgres")]
         (LogSpec::Postgres { .. }, ProjectionSpec::InMemory) => true,
+        #[cfg(feature = "postgres")]
+        (LogSpec::Postgres { .. }, ProjectionSpec::Sqlite { .. }) => true,
+        #[cfg(feature = "postgres")]
+        (LogSpec::Postgres { .. }, ProjectionSpec::Postgres { .. }) => true,
         _ => false,
     };
     if !wired {
diff --git a/.ddx/executions/20260709T012941-11bad071/report.md b/.ddx/executions/20260709T012941-11bad071/report.md
new file mode 100644
index 00000000..c5a6feb5
--- /dev/null
+++ b/.ddx/executions/20260709T012941-11bad071/report.md
@@ -0,0 +1,38 @@
+# pqueue-86c8b0ee — Runtime-wire postgres/sqlite and postgres/postgres storage combos
+
+## Change summary
+
+- `crates/pqueue-server/src/lib.rs`:
+  - Added `ProjectionSpec::Postgres { url }` (cfg-gated on `feature = "postgres"`) and its `label()` arm.
+  - Added `resolve_postgres_projection(env)`, mirroring `resolve_postgres_log` (DSN from
+    `PQUEUE_POSTGRES_PROJECTION_DATABASE_URL` / `PQUEUE_PG_PROJECTION_URL` fallback, fails closed on
+    `sslmode=require` without the `tls` feature).
+  - Added two `start()` match arms:
+    - `(LogSpec::Postgres, ProjectionSpec::Sqlite)` → `ComposedBackend<PostgresLog, SqliteProjectionStore,
+      InProcessControlPlane>`, connect + recover inside `spawn_blocking`, driven through `BlockingBackend`.
+    - `(LogSpec::Postgres, ProjectionSpec::Postgres)` → `ComposedBackend<PostgresLog, PostgresRelational,
+      InProcessControlPlane>` (two independent postgres connections, non-colliding table sets), same
+      off-reactor wiring.
+- `crates/pqueue-server/src/env_config.rs`:
+  - Wired `PQUEUE_PROJECTION_BACKEND=postgres` (cfg-gated) to `resolve_postgres_projection`.
+  - Added `(postgres, sqlite)` and `(postgres, postgres)` to the wired-pairing table.
+- `crates/pqueue-server/tests/postgres_composed_projections.rs` (new): `postgres_sqlite_combo_runs_under_tokio`
+  and `postgres_postgres_combo_runs_under_tokio` — boot `start()` over each combo under `#[tokio::test]`,
+  drive push → claim → finalize over RESP with a stock redis client. Env-gated on `PQUEUE_PG_TEST_URL`
+  (LOUD-skip otherwise).
+
+## Acceptance verification
+
+1. `postgres_sqlite_combo_runs_under_tokio` — ran live against `PQUEUE_PG_TEST_URL=postgres://pqueue:pqueue@127.0.0.1:55432/pqueue`: **ok**.
+2. `postgres_postgres_combo_runs_under_tokio` — ran live against the same DB: **ok**.
+3. `rg -n 'postgres.*sqlite|postgres.*postgres' crates/pqueue-server/src/env_config.rs` — matches the new
+   wired-pairing comment (`postgres/sqlite, postgres/postgres`), confirming both combos are wired in the
+   runtime selector.
+4. `rustup run 1.92.0 cargo test -p pqueue-server --features postgres` — full crate suite green, including the
+   two new live tests (run with `PQUEUE_PG_TEST_URL` set). `cargo clippy -p pqueue-server --features postgres
+   --tests -- -D warnings` clean.
+
+## Non-scope (per bead)
+
+Live `kind` smoke and the TP-003 AC-TXN fault-injection matrix for these two combos are separate,
+already-tracked work (bead `pqueue-52e1a2ff` / R3) and are not claimed here.
diff --git a/crates/pqueue-server/src/lib.rs b/crates/pqueue-server/src/lib.rs
index 322c66e6..c8bac6b5 100644
--- a/crates/pqueue-server/src/lib.rs
+++ b/crates/pqueue-server/src/lib.rs
@@ -117,6 +117,12 @@ pub enum ProjectionSpec {
     /// barrier; the durable SQLite image is an asynchronous checkpoint that MAY lag and is caught up by
     /// object-log tail replay on recovery.
     HybridAsync { path: PathBuf },
+    /// SYNC postgres relational projection (`PostgresRelational`, atomic class) at `url`, composed against
+    /// the [`LogSpec::Postgres`] durable log. `url` is a libpq/postgres connection string; connect + recover
+    /// MUST run off the reactor (the composition root drives it through `spawn_blocking`, same as the log
+    /// axis). Requires the `postgres` cargo feature.
+    #[cfg(feature = "postgres")]
+    Postgres { url: String },
 }
 
 impl ProjectionSpec {
@@ -126,6 +132,8 @@ impl ProjectionSpec {
             ProjectionSpec::Sqlite { .. } => "sqlite",
             ProjectionSpec::Hybrid { .. } => "hybrid",
             ProjectionSpec::HybridAsync { .. } => "hybrid-async",
+            #[cfg(feature = "postgres")]
+            ProjectionSpec::Postgres { .. } => "postgres",
         }
     }
 }
@@ -627,6 +635,41 @@ pub fn resolve_postgres_log(
     Ok(LogSpec::Postgres { url, credentials })
 }
 
+/// Resolve the postgres [`ProjectionSpec`] from the runtime environment, using the env name the Helm chart's
+/// `storage.projection.postgres` axis renders. The DSN secret is `PQUEUE_POSTGRES_PROJECTION_DATABASE_URL`;
+/// `PQUEUE_PG_PROJECTION_URL` is the local/dev fallback, and the documented default is the last resort.
+///
+/// No plaintext fallback: if the DSN demands `sslmode=require` but this binary was built WITHOUT the `tls`
+/// feature, this fails at config time rather than letting the runtime silently downgrade to `NoTls`.
+///
+/// This is a pure function over an env map (no live DB, no process env) so the composition-root config
+/// layer is unit-testable.
+#[cfg(feature = "postgres")]
+pub fn resolve_postgres_projection(
+    env: &std::collections::BTreeMap<String, String>,
+) -> Result<ProjectionSpec, String> {
+    let nonempty = |key: &str| env.get(key).filter(|s| !s.is_empty()).cloned();
+    let url = nonempty("PQUEUE_POSTGRES_PROJECTION_DATABASE_URL")
+        .or_else(|| nonempty("PQUEUE_PG_PROJECTION_URL"))
+        .unwrap_or_else(|| "postgres://postgres@127.0.0.1:5432/postgres".to_string());
+
+    // Fail closed before connecting if the DSN requires TLS but this build cannot provide it.
+    let ssl_mode = pqueue_postgres::PostgresConnectConfig::new(&url)
+        .parsed_ssl_mode()
+        .map_err(|e| format!("invalid postgres DSN: {e}"))?;
+    #[cfg(not(feature = "tls"))]
+    if matches!(ssl_mode, pqueue_postgres::PostgresSslMode::Require) {
+        return Err(
+            "DSN requests sslmode=require but this binary was built without the `tls` feature; rebuild \
+             `--features postgres,tls` (no plaintext downgrade)"
+                .to_string(),
+        );
+    }
+    let _ = ssl_mode;
+
+    Ok(ProjectionSpec::Postgres { url })
+}
+
 /// The single authoritative, fully-typed runtime configuration for a pqueue server. Every knob the server
 /// needs lives here as a typed field; there is exactly ONE optional env populator (`Config::from_env`, in
 /// the `pqueue-service` bin) that maps the documented `PQUEUE_*`/`DATABRICKS_*` env names onto these fields.
@@ -1471,6 +1514,66 @@ pub async fn start(config: Config) -> EngineResult<Server> {
             let backend = Arc::new(BlockingBackend::from_arc(Arc::new(backend)));
             run_owned(backend, node_id, clock, &listen, interval, &queues).await
         }
+        #[cfg(feature = "postgres")]
+        (LogSpec::Postgres { url, credentials }, ProjectionSpec::Sqlite { path }) => {
+            // The composed postgres-log + sqlite-projection backend (`ComposedBackend<PostgresLog,
+            // SqliteProjectionStore, InProcessControlPlane>`): the durable postgres command log paired with a
+            // derived SQLite relational projection, recovery-on-open. Same off-reactor discipline as
+            // postgres/inmemory above: connect BOTH axes and recover inside `spawn_blocking`, then drive the
+            // composition only through the blocking-safe `BlockingBackend` wrapper.
+            let p = path
+                .to_str()
+                .ok_or_else(|| EngineError::Storage("non-utf8 path".into()))?
+                .to_string();
+            let backend = tokio::task::spawn_blocking(move || {
+                let mut connect_config = pqueue_postgres::PostgresConnectConfig::new(url);
+                if let Some(provider) = credentials {
+                    connect_config = connect_config.with_credential_provider(provider);
+                }
+                let log = pqueue_postgres::PostgresLog::connect_with_config(connect_config)?;
+                let projection = pqueue_sqlite::SqliteProjectionStore::open(&p)?;
+                ComposedBackend::new(log, projection, InProcessControlPlane::new())
+                    .recover()
+                    .map(|b| b.with_node_id(node_id))
+            })
+            .await
+            .map_err(|e| {
+                EngineError::Storage(format!("postgres/sqlite connect task join failed: {e}"))
+            })??;
+            let backend = Arc::new(BlockingBackend::from_arc(Arc::new(backend)));
+            run_owned(backend, node_id, clock, &listen, interval, &queues).await
+        }
+        #[cfg(feature = "postgres")]
+        (
+            LogSpec::Postgres { url, credentials },
+            ProjectionSpec::Postgres {
+                url: projection_url,
+            },
+        ) => {
+            // The composed postgres-log + postgres-projection backend (`ComposedBackend<PostgresLog,
+            // PostgresRelational, InProcessControlPlane>`): the durable postgres command log paired with a
+            // SEPARATE postgres connection driving the relational projection (distinct table sets, no
+            // collision — see `pqueue_postgres::compose_log`'s `log_entries`/`queue_defs` vs
+            // `pqueue_postgres::relational`'s `pqueue_items`/`queues`), recovery-on-open. Same off-reactor
+            // discipline: connect BOTH axes and recover inside `spawn_blocking`.
+            let backend = tokio::task::spawn_blocking(move || {
+                let mut connect_config = pqueue_postgres::PostgresConnectConfig::new(url);
+                if let Some(provider) = credentials {
+                    connect_config = connect_config.with_credential_provider(provider);
+                }
+                let log = pqueue_postgres::PostgresLog::connect_with_config(connect_config)?;
+                let projection = pqueue_postgres::PostgresRelational::connect(&projection_url)?;
+                ComposedBackend::new(log, projection, InProcessControlPlane::new())
+                    .recover()
+                    .map(|b| b.with_node_id(node_id))
+            })
+            .await
+            .map_err(|e| {
+                EngineError::Storage(format!("postgres/postgres connect task join failed: {e}"))
+            })??;
+            let backend = Arc::new(BlockingBackend::from_arc(Arc::new(backend)));
+            run_owned(backend, node_id, clock, &listen, interval, &queues).await
+        }
         (log, projection) => Err(EngineError::Storage(format!(
             "unsupported backend composition: log={} projection={} (not wired by pqueue-server)",
             log.label(),
diff --git a/crates/pqueue-server/tests/postgres_composed_projections.rs b/crates/pqueue-server/tests/postgres_composed_projections.rs
new file mode 100644
index 00000000..c5368208
--- /dev/null
+++ b/crates/pqueue-server/tests/postgres_composed_projections.rs
@@ -0,0 +1,216 @@
+//! Runtime wiring for the postgres-log × {sqlite, postgres}-projection combos (ADR-012 P2 gap-closure).
+//!
+//! Both combos assemble their composed backend the SAME off-reactor way as the already-wired
+//! postgres/inmemory combo (`crates/pqueue-server/tests/postgres_native.rs`): connect + recover inside
+//! `spawn_blocking`, then drive every port through the blocking-safe `BlockingBackend` wrapper so no sync
+//! postgres client call ever runs on a Tokio reactor worker (it would panic — "cannot start a runtime from
+//! within a runtime"). These tests boot the full server over each combo and drive push → claim → finalize
+//! over RESP, proving the composition survives a real `#[tokio::test]` runtime end to end.
+//!
+//! Env-gated on `PQUEUE_PG_TEST_URL`; LOUD-skips (not silently) when no live database is configured.
+#![cfg(feature = "postgres")]
+
+use pqueue_core::{
+    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
+    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
+};
+use pqueue_server::{BackendSpec, Config, ControlPlaneSpec, LogSpec, ProjectionSpec, start};
+use std::time::Duration;
+
+fn qdef() -> QueueDefinition {
+    QueueDefinition {
+        tenant_id: TenantId::new("t1").unwrap(),
+        queue_id: QueueId::new("q1").unwrap(),
+        priority_model: PriorityModel {
+            kind: PriorityModelKind::Int64,
+            direction: PriorityDirection::Ascending,
+            tie_breaker: PriorityTieBreaker::CreatedSequence,
+        },
+        ordering_mode: OrderingMode::Strict,
+        max_rank_error: 0,
+        progress_bound_ms: 60_000,
+        eligibility_policy: EligibilityPolicy::default(),
+        cohort_policy: None,
+        recurrence: RecurrencePolicy::default(),
+        request_id_retention_ms: 60_000,
+        client_item_key_retention_ms: 60_000,
+        terminal_retention_ms: 60_000,
+        max_lease_duration_ms: 60_000,
+        retry_policy: RetryPolicy { max_attempts: 3 },
+        max_push_batch_size: 100,
+        max_claim_batch_size: 100,
+        max_eligible_group_size: None,
+        secondary_indexes: vec![],
+        entity_schema: None,
+        typed_indexes: vec![],
+        emit_change_records: true,
+    }
+}
+
+/// A unique `?options=-csearch_path=<schema>` DSN, so parallel/rerun test runs never collide on the shared
+/// queue tables — same trick `postgres_native.rs`'s live smoke uses for the log axis.
+fn url_with_schema(url: &str, schema: &str) -> String {
+    if url.contains("?options=") || url.contains("&options=") {
+        url.to_string()
+    } else if url.contains('?') {
+        format!("{url}&options=-csearch_path%3D{schema}")
+    } else {
+        format!("{url}?options=-csearch_path%3D{schema}")
+    }
+}
+
+async fn create_schema(url: &str, schema: &str) {
+    let create = url.to_string();
+    let schema = schema.to_string();
+    tokio::task::spawn_blocking(move || {
+        let mut client =
+            pqueue_postgres::connect(pqueue_postgres::PostgresConnectConfig::new(create))
+                .expect("connect to create schema");
+        client
+            .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema};"))
+            .expect("create schema");
+    })
+    .await
+    .unwrap();
+}
+
+async fn drop_schema(url: &str, schema: &str) {
+    let drop_url = url.to_string();
+    let drop_schema = schema.to_string();
+    let _ = tokio::task::spawn_blocking(move || {
+        if let Ok(mut client) =
+            pqueue_postgres::connect(pqueue_postgres::PostgresConnectConfig::new(drop_url))
+        {
+            let _ = client.batch_execute(&format!("DROP SCHEMA IF EXISTS {drop_schema} CASCADE;"));
+        }
+    })
+    .await;
+}
+
+async fn push_claim_finalize_over_resp(addr: std::net::SocketAddr) {
+    let client = redis::Client::open(format!("redis://{addr}")).unwrap();
+    let mut con = client.get_multiplexed_async_connection().await.unwrap();
+
+    let produced: String = redis::cmd("XADD")
+        .arg("t1:q1")
+        .arg("*")
+        .arg("priority")
+        .arg(7)
+        .query_async(&mut con)
+        .await
+        .unwrap();
+
+    let reply: redis::streams::StreamReadReply = redis::cmd("XREADGROUP")
+        .arg("GROUP")
+        .arg("g")
+        .arg("c")
+        .arg("STREAMS")
+        .arg("t1:q1")
+        .arg(">")
+        .query_async(&mut con)
+        .await
+        .unwrap();
+    assert_eq!(reply.keys[0].ids.len(), 1, "claim returns the pushed item");
+    assert_eq!(reply.keys[0].ids[0].id, produced);
+
+    let acked: i64 = redis::cmd("XACK")
+        .arg("t1:q1")
+        .arg("g")
+        .arg(&produced)
+        .query_async(&mut con)
+        .await
+        .unwrap();
+    assert_eq!(acked, 1, "finalize (ack) commits the claimed item");
+}
+
+/// Composed postgres-log + sqlite-projection backend, driven end to end under a real Tokio runtime: proves
+/// the `spawn_blocking` + `BlockingBackend` boundary covers this combo the same way it covers postgres/inmemory
+/// (no reactor-thread panic on the sync postgres `connect`/`recover`).
+#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
+async fn postgres_sqlite_combo_runs_under_tokio() {
+    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
+        eprintln!(
+            "POSTGRES/SQLITE COMBO SMOKE SKIPPED (push/claim/finalize) — set PQUEUE_PG_TEST_URL to a live DB"
+        );
+        return;
+    };
+    let schema = format!("pq_pgsqlite_{}", std::process::id());
+    let scoped_url = url_with_schema(&url, &schema);
+    create_schema(&url, &schema).await;
+
+    let sqlite_path = std::env::temp_dir().join(format!(
+        "pqueue-server-postgres-sqlite-combo-{}-projection.db",
+        std::process::id()
+    ));
+    let _ = std::fs::remove_file(&sqlite_path);
+
+    let backend = BackendSpec {
+        log: LogSpec::Postgres {
+            url: scoped_url,
+            credentials: None,
+        },
+        projection: ProjectionSpec::Sqlite {
+            path: sqlite_path.clone(),
+        },
+        control_plane: ControlPlaneSpec::InProcess,
+    };
+    let server = start(Config::new(
+        backend,
+        0,
+        "127.0.0.1:0".to_string(),
+        Duration::from_secs(60),
+        vec![qdef()],
+    ))
+    .await
+    .expect("postgres/sqlite combo server starts under tokio against a live DB");
+
+    push_claim_finalize_over_resp(server.addr()).await;
+
+    server.shutdown_and_drain(Duration::from_secs(5)).await;
+    let _ = std::fs::remove_file(&sqlite_path);
+    drop_schema(&url, &schema).await;
+}
+
+/// Composed postgres-log + postgres-projection backend (two independent postgres connections — one per
+/// axis, non-colliding table sets), driven end to end under a real Tokio runtime.
+#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
+async fn postgres_postgres_combo_runs_under_tokio() {
+    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
+        eprintln!(
+            "POSTGRES/POSTGRES COMBO SMOKE SKIPPED (push/claim/finalize) — set PQUEUE_PG_TEST_URL to a live DB"
+        );
+        return;
+    };
+    let log_schema = format!("pq_pgpg_log_{}", std::process::id());
+    let projection_schema = format!("pq_pgpg_proj_{}", std::process::id());
+    let log_url = url_with_schema(&url, &log_schema);
+    let projection_url = url_with_schema(&url, &projection_schema);
+    create_schema(&url, &log_schema).await;
+    create_schema(&url, &projection_schema).await;
+
+    let backend = BackendSpec {
+        log: LogSpec::Postgres {
+            url: log_url,
+            credentials: None,
+        },
+        projection: ProjectionSpec::Postgres {
+            url: projection_url,
+        },
+        control_plane: ControlPlaneSpec::InProcess,
+    };
+    let server = start(Config::new(
+        backend,
+        0,
+        "127.0.0.1:0".to_string(),
+        Duration::from_secs(60),
+        vec![qdef()],
+    ))
+    .await
+    .expect("postgres/postgres combo server starts under tokio against a live DB");
+
+    push_claim_finalize_over_resp(server.addr()).await;
+
+    server.shutdown_and_drain(Duration::from_secs(5)).await;
+    drop_schema(&url, &log_schema).await;
+    drop_schema(&url, &projection_schema).await;
+}
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
