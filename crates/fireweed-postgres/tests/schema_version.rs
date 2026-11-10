//! Stored schema version, live: every PostgreSQL DDL family is stamped at first open and a mismatch is
//! refused at open. Gated on `FIREWEED_PG_TEST_URL` like the crate's other live suites (fail-closed).
//! The queue store uses a fixed PostgreSQL schema, so its cases run in a throwaway database created
//! on the same server.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fireweed_core::queue_contract::{QueueLimits, timestamp_from_nanos};
use fireweed_engine::{Clock, ControlPlaneConfig};
use fireweed_postgres::{
    AsyncPostgresControlPlane, PostgresBackend, PostgresControlPlane, PostgresLog,
    PostgresQueueStore, PostgresRelational, PostgresRelationalBackend,
};
use postgres::{Client, NoTls};

fn base_url() -> String {
    std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL required (fail-closed live postgres)")
}

fn admin() -> Client {
    Client::connect(&base_url(), NoTls).expect("connect admin")
}

fn nonce() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}_{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        timestamp_from_nanos(0)
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

type Open = fn(&str, &str) -> Result<(), String>;

fn open_relational_backend(url: &str, schema: &str) -> Result<(), String> {
    PostgresRelationalBackend::connect_in_schema(url, schema)
        .map(drop)
        .map_err(|error| format!("{error:?}"))
}
fn open_relational(url: &str, schema: &str) -> Result<(), String> {
    PostgresRelational::connect_in_schema(url, schema)
        .map(drop)
        .map_err(|error| format!("{error:?}"))
}
fn open_control_plane(url: &str, schema: &str) -> Result<(), String> {
    PostgresControlPlane::connect_in_schema(url, schema, ControlPlaneConfig::default())
        .map(drop)
        .map_err(|error| format!("{error:?}"))
}
fn open_async_control_plane(url: &str, schema: &str) -> Result<(), String> {
    runtime().block_on(async {
        let plane = AsyncPostgresControlPlane::connect_in_schema(
            url,
            schema,
            ControlPlaneConfig::default(),
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
        plane.shutdown().await.map_err(|error| format!("{error:?}"))
    })
}
fn open_backend(url: &str, schema: &str) -> Result<(), String> {
    PostgresBackend::connect_in_schema(url, schema)
        .map(drop)
        .map_err(|error| format!("{error:?}"))
}
fn open_log(url: &str, schema: &str) -> Result<(), String> {
    PostgresLog::connect_in_schema(url, schema)
        .map(drop)
        .map_err(|error| format!("{error:?}"))
}
/// The queue store ignores `schema`: its tables live in the fixed `fireweed_api007` schema of whichever
/// database `url` names.
fn open_queue_store(url: &str, _schema: &str) -> Result<(), String> {
    runtime().block_on(async {
        PostgresQueueStore::connect(url, Arc::new(FixedClock), QueueLimits::default(), 1)
            .await
            .map(drop)
            .map_err(|error| format!("{error:?}"))
    })
}

struct Case {
    /// Stored component name the case opens.
    component: &'static str,
    open: Open,
    /// PostgreSQL schema the stored version table lives in, given the case's `schema` argument.
    fixed_schema: Option<&'static str>,
}

const CASES: [Case; 8] = [
    Case {
        component: "relational",
        open: open_relational_backend,
        fixed_schema: None,
    },
    Case {
        component: "relational_command_log",
        open: open_relational_backend,
        fixed_schema: None,
    },
    Case {
        component: "relational_queue_metrics",
        open: open_relational,
        fixed_schema: None,
    },
    Case {
        component: "control_plane",
        open: open_control_plane,
        fixed_schema: None,
    },
    Case {
        component: "control_plane",
        open: open_async_control_plane,
        fixed_schema: None,
    },
    Case {
        component: "postgres_backend",
        open: open_backend,
        fixed_schema: None,
    },
    Case {
        component: "compose_log",
        open: open_log,
        fixed_schema: None,
    },
    Case {
        component: "queue_store",
        open: open_queue_store,
        fixed_schema: Some("fireweed_api007"),
    },
];

/// A scratch target: a fresh PostgreSQL schema, or for the fixed-schema queue store a throwaway database.
struct Target {
    url: String,
    schema: String,
    version_schema: String,
    database: Option<String>,
}

impl Target {
    fn new(case: &Case) -> Self {
        let id = nonce();
        match case.fixed_schema {
            None => {
                let schema = format!("fw_sv_{id}");
                Self {
                    url: base_url(),
                    version_schema: schema.clone(),
                    schema,
                    database: None,
                }
            }
            Some(fixed) => {
                let database = format!("fw_sv_{id}");
                admin()
                    .batch_execute(&format!("CREATE DATABASE {database}"))
                    .expect("create database");
                let base = base_url();
                let (head, query) = base
                    .split_once('?')
                    .map_or((base.as_str(), ""), |(h, q)| (h, q));
                let query = if query.is_empty() {
                    String::new()
                } else {
                    format!("?{query}")
                };
                let root = head.rsplit_once('/').unwrap().0;
                Self {
                    url: format!("{root}/{database}{query}"),
                    schema: String::new(),
                    version_schema: fixed.into(),
                    database: Some(database),
                }
            }
        }
    }

    fn client(&self) -> Client {
        Client::connect(&self.url, NoTls).expect("connect target")
    }

    fn rows(&self, component: &str) -> Vec<i64> {
        let mut client = self.client();
        client
            .query(
                &format!(
                    "SELECT version FROM {}.fireweed_schema_version WHERE component=$1",
                    self.version_schema
                ),
                &[&component],
            )
            .unwrap()
            .iter()
            .map(|row| row.get(0))
            .collect()
    }

    fn exec(&self, sql: &str) {
        self.client().batch_execute(sql).unwrap();
    }

    fn open(&self, case: &Case) -> Result<(), String> {
        (case.open)(&self.url, &self.schema)
    }
}

impl Drop for Target {
    fn drop(&mut self) {
        match &self.database {
            Some(database) => {
                let _ = admin()
                    .batch_execute(&format!("DROP DATABASE IF EXISTS {database} WITH (FORCE)"));
            }
            None => {
                let _ = admin()
                    .batch_execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema));
            }
        }
    }
}

/// The version a component's current DDL is stamped with.
fn required(component: &str) -> i64 {
    if component == "queue_store" { 2 } else { 1 }
}
fn assert_refusal(message: &str, case: &Case, found: &str, target: &Target) {
    assert!(message.contains(case.component), "{message}");
    assert!(
        message.contains(&format!("stored version {found}")),
        "{message}"
    );
    assert!(
        message.contains(&format!("requires version {}", required(case.component))),
        "{message}"
    );
    assert!(message.contains("recreated"), "{message}");
    assert!(
        !message.contains(&target.url) && !message.contains("127.0.0.1"),
        "{message}"
    );
}

#[test]
fn fresh_open_stamps_and_reopen_succeeds() {
    for case in &CASES {
        let target = Target::new(case);
        target
            .open(case)
            .unwrap_or_else(|error| panic!("{}: {error}", case.component));
        assert_eq!(
            target.rows(case.component),
            vec![required(case.component)],
            "{}",
            case.component
        );
        target
            .open(case)
            .unwrap_or_else(|error| panic!("{} reopen: {error}", case.component));
        assert_eq!(
            target.rows(case.component),
            vec![required(case.component)],
            "{}",
            case.component
        );
    }
}

#[test]
fn wrong_version_is_refused_before_any_table_is_touched() {
    for case in &CASES {
        let target = Target::new(case);
        target.open(case).unwrap();
        target.exec(&format!(
            "UPDATE {}.fireweed_schema_version SET version=999 WHERE component='{}'",
            target.version_schema, case.component
        ));
        let error = refused_with_tables_locked(&target, case);
        assert_refusal(&error, case, "999", &target);
        assert_eq!(target.rows(case.component), vec![999]);
    }
}

#[test]
fn tables_without_a_version_row_are_refused_before_any_table_is_touched() {
    for case in &CASES {
        let target = Target::new(case);
        target.open(case).unwrap();
        target.exec(&format!(
            "DELETE FROM {}.fireweed_schema_version WHERE component='{}'",
            target.version_schema, case.component
        ));
        let error = refused_with_tables_locked(&target, case);
        assert_refusal(&error, case, "none", &target);
        assert!(target.rows(case.component).is_empty());
    }
}

#[test]
fn tables_from_before_the_version_table_existed_are_refused_without_creating_it() {
    for case in &CASES {
        let target = Target::new(case);
        target.open(case).unwrap();
        target.exec(&format!(
            "DROP TABLE {}.fireweed_schema_version",
            target.version_schema
        ));
        let error = target.open(case).unwrap_err();
        // Every component of the opener is unstamped; the first one is named.
        assert!(error.contains("stored version none"), "{error}");
        assert!(error.contains("recreated"), "{error}");
        let exists: bool = target
            .client()
            .query_one(
                &format!(
                    "SELECT to_regclass('{}.fireweed_schema_version') IS NOT NULL",
                    target.version_schema
                ),
                &[],
            )
            .unwrap()
            .get(0);
        assert!(
            !exists,
            "a refused open must not leave the version table behind"
        );
    }
}

/// Holds ACCESS EXCLUSIVE on every component table while the open runs on another thread. An open that
/// touched any of them would block; a refusal that precedes them returns promptly.
fn refused_with_tables_locked(target: &Target, case: &Case) -> String {
    let mut holder = target.client();
    let mut tx = holder.transaction().unwrap();
    let tables: Vec<String> = tx
        .query(
            "SELECT schemaname||'.'||tablename FROM pg_tables \
             WHERE schemaname=$1 AND tablename<>'fireweed_schema_version'",
            &[&target.version_schema],
        )
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert!(!tables.is_empty());
    for table in &tables {
        tx.batch_execute(&format!("LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"))
            .unwrap();
    }
    let (sender, receiver) = std::sync::mpsc::channel();
    let (url, schema, open) = (target.url.clone(), target.schema.clone(), case.open);
    let worker = std::thread::spawn(move || sender.send(open(&url, &schema)).unwrap());
    let outcome = receiver.recv_timeout(Duration::from_secs(20));
    tx.rollback().unwrap();
    worker.join().unwrap();
    outcome
        .unwrap_or_else(|_| panic!("{}: open blocked on a component table", case.component))
        .expect_err("open must be refused")
}

#[test]
fn concurrent_fresh_opens_all_succeed_with_one_row() {
    for case in &CASES {
        let target = Target::new(case);
        if case.fixed_schema.is_none() {
            // Schema creation itself is not serialized by every constructor; this test is about the
            // version stamp, so the PostgreSQL schema exists before the opens race.
            admin()
                .batch_execute(&format!("CREATE SCHEMA {}", target.schema))
                .unwrap();
        }
        let handles: Vec<_> = (0..6)
            .map(|_| {
                let (url, schema, open) = (target.url.clone(), target.schema.clone(), case.open);
                std::thread::spawn(move || open(&url, &schema))
            })
            .collect();
        for handle in handles {
            handle
                .join()
                .unwrap()
                .unwrap_or_else(|error| panic!("{}: {error}", case.component));
        }
        assert_eq!(
            target.rows(case.component),
            vec![required(case.component)],
            "{}",
            case.component
        );
    }
}

#[test]
fn components_sharing_one_schema_keep_separate_rows() {
    let schema = format!("fw_sv_shared_{}", nonce());
    let url = base_url();
    open_relational_backend(&url, &schema).unwrap();
    open_relational(&url, &schema).unwrap();
    open_log(&url, &schema).unwrap();
    open_control_plane(&url, &schema).unwrap();
    let mut client = admin();
    let names: Vec<String> = client
        .query(
            &format!("SELECT component FROM {schema}.fireweed_schema_version ORDER BY component"),
            &[],
        )
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .unwrap();
    assert_eq!(
        names,
        [
            "compose_log",
            "control_plane",
            "relational",
            "relational_command_log",
            "relational_queue_metrics"
        ]
    );
}

#[test]
fn concurrent_sync_and_async_control_plane_opens_share_one_stamp() {
    for round in 0..5 {
        let schema = format!("fw_sv_cp_{round}_{}", nonce());
        admin()
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .unwrap();
        let url = base_url();
        let handles: Vec<_> = (0..8)
            .map(|index| {
                let (url, schema) = (url.clone(), schema.clone());
                std::thread::spawn(move || {
                    if index % 2 == 0 {
                        open_control_plane(&url, &schema)
                    } else {
                        open_async_control_plane(&url, &schema)
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap().unwrap();
        }
        let mut client = admin();
        let count: i64 = client
            .query_one(
                &format!("SELECT count(*) FROM {schema}.fireweed_schema_version WHERE component='control_plane'"),
                &[],
            )
            .unwrap()
            .get(0);
        client
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .unwrap();
        assert_eq!(count, 1);
    }
}
