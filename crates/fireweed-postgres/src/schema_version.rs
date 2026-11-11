//! Stored schema versions for the PostgreSQL stores.
//!
//! Schema setup is `CREATE ... IF NOT EXISTS`, so a database created by an older build keeps its old
//! tables. Each DDL family is therefore a named component with a version, stored in one
//! `fireweed_schema_version` table per PostgreSQL schema. At open, under an advisory lock and in one
//! transaction, a component whose tables are absent is created and stamped together; a stored version
//! equal to this build's continues; anything else is refused. There is no migration: a refused database
//! must be recreated. To change a component's DDL, bump its version constant and update the digest pinned
//! in the `ddl_digests_pin_component_versions` test.

pub(crate) const VERSION_TABLE: &str = "fireweed_schema_version";

pub(crate) const QUEUE_STORE_SCHEMA_VERSION: i64 = 3;
pub(crate) const RELATIONAL_SCHEMA_VERSION: i64 = 1;
pub(crate) const COMMAND_LOG_SCHEMA_VERSION: i64 = 1;
pub(crate) const QUEUE_METRICS_SCHEMA_VERSION: i64 = 1;
pub(crate) const CONTROL_PLANE_SCHEMA_VERSION: i64 = 1;
pub(crate) const BACKEND_SCHEMA_VERSION: i64 = 1;
pub(crate) const COMPOSE_LOG_SCHEMA_VERSION: i64 = 1;

/// One DDL family. `sentinel` is a table only this component creates, so its presence without a stored
/// row identifies tables from a build that predates the version check.
pub(crate) struct SchemaComponent {
    pub(crate) name: &'static str,
    pub(crate) version: i64,
    /// A PostgreSQL schema the component qualifies its tables with; `None` uses the session `search_path`.
    pub(crate) schema: Option<&'static str>,
    pub(crate) sentinel: &'static str,
    pub(crate) ddl: &'static str,
}

pub(crate) const QUEUE_STORE: SchemaComponent = SchemaComponent {
    name: "queue_store",
    version: QUEUE_STORE_SCHEMA_VERSION,
    schema: Some("fireweed_api007"),
    sentinel: "fireweed_api007.queues",
    ddl: crate::queue_store::DDL,
};
pub(crate) const RELATIONAL: SchemaComponent = SchemaComponent {
    name: "relational",
    version: RELATIONAL_SCHEMA_VERSION,
    schema: None,
    sentinel: "fireweed_items",
    ddl: crate::relational::RELATIONAL_SCHEMA,
};
pub(crate) const COMMAND_LOG: SchemaComponent = SchemaComponent {
    name: "relational_command_log",
    version: COMMAND_LOG_SCHEMA_VERSION,
    schema: None,
    sentinel: "fireweed_commands",
    ddl: crate::relational::COMMAND_LOG_SCHEMA,
};
pub(crate) const QUEUE_METRICS: SchemaComponent = SchemaComponent {
    name: "relational_queue_metrics",
    version: QUEUE_METRICS_SCHEMA_VERSION,
    schema: None,
    sentinel: "fireweed_queue_metrics_v2",
    ddl: crate::relational::QUEUE_METRICS_SCHEMA,
};
pub(crate) const CONTROL_PLANE: SchemaComponent = SchemaComponent {
    name: "control_plane",
    version: CONTROL_PLANE_SCHEMA_VERSION,
    schema: None,
    sentinel: "fireweed_workers",
    ddl: crate::control_plane::CONTROL_PLANE_SCHEMA,
};
pub(crate) const BACKEND: SchemaComponent = SchemaComponent {
    name: "postgres_backend",
    version: BACKEND_SCHEMA_VERSION,
    schema: None,
    sentinel: "request_idempotency",
    ddl: crate::SCHEMA,
};
pub(crate) const COMPOSE_LOG: SchemaComponent = SchemaComponent {
    name: "compose_log",
    version: COMPOSE_LOG_SCHEMA_VERSION,
    schema: None,
    sentinel: "log_epochs",
    ddl: crate::compose_log::SCHEMA,
};

/// Why opening a component failed. `Refused` carries the operator-facing message, which never includes
/// connection details.
pub(crate) enum GateError<E> {
    Refused(String),
    Db(E),
}

pub(crate) fn into_engine<E: std::fmt::Display>(
    error: GateError<E>,
) -> fireweed_engine::EngineError {
    match error {
        GateError::Refused(message) => fireweed_engine::EngineError::Storage(message),
        GateError::Db(error) => fireweed_engine::EngineError::Storage(error.to_string()),
    }
}

/// What the caller must do for one component inside the open transaction.
pub(crate) struct Gate {
    /// Run the component's DDL (its tables are absent).
    pub(crate) run_ddl: bool,
    /// Write the version row after the DDL (no row exists yet).
    pub(crate) stamp: bool,
}

impl SchemaComponent {
    fn qualified(&self, table: &str) -> String {
        match self.schema {
            Some(schema) => format!("{schema}.{table}"),
            None => table.to_string(),
        }
    }
    fn version_table(&self) -> String {
        self.qualified(VERSION_TABLE)
    }
    fn create_version_table_sql(&self) -> String {
        format!(
            "{}CREATE TABLE IF NOT EXISTS {} (component TEXT PRIMARY KEY, version BIGINT NOT NULL)",
            self.schema
                .map(|schema| format!("CREATE SCHEMA IF NOT EXISTS {schema}; "))
                .unwrap_or_default(),
            self.version_table()
        )
    }
    fn exists_sql(&self, table: &str) -> String {
        format!("SELECT to_regclass('{table}') IS NOT NULL")
    }
    fn read_sql(&self) -> String {
        format!(
            "SELECT version FROM {} WHERE component=$1",
            self.version_table()
        )
    }
    fn stamp_sql(&self) -> String {
        format!(
            "INSERT INTO {} (component, version) VALUES ($1, $2)",
            self.version_table()
        )
    }
    fn refusal(&self, found: Option<i64>) -> String {
        let found = found.map_or_else(|| "none".to_string(), |version| version.to_string());
        format!(
            "PostgreSQL schema component `{}` has stored version {found} but this build requires \
             version {}; the database must be recreated (there is no migration)",
            self.name, self.version
        )
    }
    fn decide(&self, found: Option<i64>, tables_present: bool) -> Result<Gate, String> {
        match (found, tables_present) {
            (None, false) => Ok(Gate {
                run_ddl: true,
                stamp: true,
            }),
            (Some(version), present) if version == self.version => Ok(Gate {
                run_ddl: !present,
                stamp: false,
            }),
            (found, _) => Err(self.refusal(found)),
        }
    }
}

/// Serializes first-time setup of one PostgreSQL schema across processes.
const LOCK_SQL: &str = "SELECT pg_advisory_xact_lock(hashtextextended('fireweed-schema-version:' || current_database() || ':' || current_schema(), 0))";

/// Blocking open for a set of components sharing the session's schema. Refusal is decided for every
/// component before any DDL runs, and the DDL and version rows commit together.
pub(crate) fn open_blocking(
    client: &mut postgres::Client,
    components: &[&SchemaComponent],
) -> Result<Vec<Gate>, GateError<postgres::Error>> {
    let mut tx = client.transaction().map_err(GateError::Db)?;
    tx.batch_execute(LOCK_SQL).map_err(GateError::Db)?;
    let mut gates = Vec::with_capacity(components.len());
    for component in components {
        gates.push(gate_blocking(&mut tx, component)?);
    }
    for (component, gate) in components.iter().zip(&gates) {
        if gate.run_ddl {
            tx.batch_execute(component.ddl).map_err(GateError::Db)?;
        }
        if gate.stamp {
            stamp_blocking(&mut tx, component)?;
        }
    }
    tx.commit().map_err(GateError::Db)?;
    Ok(gates)
}

fn gate_blocking(
    tx: &mut postgres::Transaction<'_>,
    component: &SchemaComponent,
) -> Result<Gate, GateError<postgres::Error>> {
    tx.batch_execute(&component.create_version_table_sql())
        .map_err(GateError::Db)?;
    let found: Option<i64> = tx
        .query_opt(&component.read_sql(), &[&component.name])
        .map_err(GateError::Db)?
        .map(|row| row.get(0));
    let present: bool = tx
        .query_one(&component.exists_sql(component.sentinel), &[])
        .map_err(GateError::Db)?
        .get(0);
    component.decide(found, present).map_err(GateError::Refused)
}

fn stamp_blocking(
    tx: &mut postgres::Transaction<'_>,
    component: &SchemaComponent,
) -> Result<(), GateError<postgres::Error>> {
    tx.execute(
        &component.stamp_sql(),
        &[&component.name, &component.version],
    )
    .map_err(GateError::Db)?;
    Ok(())
}

/// Async check inside the caller's transaction, after the caller took its own lock and set `search_path`. Creates the version
/// table (and the component's schema) when absent, then decides. The caller runs the DDL when
/// `run_ddl`, then [`stamp`] when `stamp`, in the same transaction.
pub(crate) async fn gate(
    tx: &impl tokio_postgres::GenericClient,
    component: &SchemaComponent,
) -> Result<Gate, GateError<tokio_postgres::Error>> {
    // The same lock `open_blocking` takes, so blocking and async opens of one schema serialize.
    // The caller has already taken its own lock and set `search_path`.
    tx.batch_execute(LOCK_SQL).await.map_err(GateError::Db)?;
    tx.batch_execute(&component.create_version_table_sql())
        .await
        .map_err(GateError::Db)?;
    let found: Option<i64> = tx
        .query_opt(&component.read_sql(), &[&component.name])
        .await
        .map_err(GateError::Db)?
        .map(|row| row.get(0));
    let present: bool = tx
        .query_one(&component.exists_sql(component.sentinel), &[])
        .await
        .map_err(GateError::Db)?
        .get(0);
    component.decide(found, present).map_err(GateError::Refused)
}

pub(crate) async fn stamp(
    tx: &impl tokio_postgres::GenericClient,
    component: &SchemaComponent,
) -> Result<(), GateError<tokio_postgres::Error>> {
    tx.execute(
        &component.stamp_sql(),
        &[&component.name, &component.version],
    )
    .await
    .map_err(GateError::Db)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn all() -> [&'static SchemaComponent; 7] {
        [
            &QUEUE_STORE,
            &RELATIONAL,
            &COMMAND_LOG,
            &QUEUE_METRICS,
            &CONTROL_PLANE,
            &BACKEND,
            &COMPOSE_LOG,
        ]
    }

    /// (component, pinned version, SHA-256 of the DDL text).
    const PINS: [(&str, i64, &str); 7] = [
        (
            "queue_store",
            3,
            "470a822eff69a564805fc15e1e77819837feea2c04109673bd52c1000e9982dc",
        ),
        (
            "relational",
            1,
            "83660dd9ed24848fe9bcb0f8467a78893e4c83ee888e0dd85bfa5ce39e861b0b",
        ),
        (
            "relational_command_log",
            1,
            "5bed97b834819da8a8087ea4637e7061862c003454fcd0e6d2f66663f66de64d",
        ),
        (
            "relational_queue_metrics",
            1,
            "266eab2ecfc14edbf4994c43e03c7778b948e4fa7161ab6576f27a744be0cf2d",
        ),
        (
            "control_plane",
            1,
            "b05090ee97d0489e5b52a7d175ad92b214cd812883306333d3d2c34767fcbcae",
        ),
        (
            "postgres_backend",
            1,
            "81fd27f4b1560ed8e81a5fbbd5d24f98385a9d05bf012f4677b51a4a9e8e0cf9",
        ),
        (
            "compose_log",
            1,
            "33b675e62e62387856ed64a55f224e78f7dd921beb1383a1595ad18e52f59835",
        ),
    ];

    #[test]
    fn ddl_digests_pin_component_versions() {
        for component in all() {
            let digest = Sha256::digest(component.ddl.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let pin = PINS
                .iter()
                .find(|pin| pin.0 == component.name)
                .unwrap_or_else(|| panic!("component {} has no pin", component.name));
            assert!(
                pin.1 == component.version && pin.2 == digest,
                "schema component `{}` changed (version {}, DDL sha256 {digest}). If the DDL changed, \
                 bump its *_SCHEMA_VERSION constant in schema_version.rs and update the pin in PINS \
                 to the new version and digest.",
                component.name,
                component.version,
            );
        }
    }

    #[test]
    fn component_names_are_unique_and_decisions_follow_the_rules() {
        let mut names: Vec<_> = all().iter().map(|component| component.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), all().len());
        let c = &RELATIONAL;
        assert!(matches!(
            c.decide(None, false),
            Ok(Gate {
                run_ddl: true,
                stamp: true
            })
        ));
        assert!(matches!(
            c.decide(Some(c.version), true),
            Ok(Gate {
                run_ddl: false,
                stamp: false
            })
        ));
        assert!(c.decide(None, true).is_err());
        let message = c.decide(Some(c.version + 1), true).err().unwrap();
        assert!(message.contains("relational") && message.contains("recreated"));
    }
}
