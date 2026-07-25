//! Destructive service boundaries for the performance matrix.
//!
//! Cleanup capabilities are minted from an exact run namespace. Callers cannot
//! construct a prefix-only deletion request or point local cleanup outside the
//! canonical run directory.

use std::path::{Path, PathBuf};

use fireweed_objectlog::segmented::{BlobStore, S3BlobStore};
use postgres::{Client, NoTls};
use sha2::{Digest, Sha256};

const LOCK_NAME: &str = "fireweed-performance-matrix-v1";
const LOCK_KEY: &str = "fireweed-perf/v1/_locks/matrix.lock";

#[derive(Clone)]
pub struct PostgresService {
    pub url: String,
}

#[derive(Clone)]
pub struct ObjectStoreService {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access: String,
    pub secret: String,
}

/// Redacts configured values before an error is written to stderr or evidence.
pub struct SecretRedactor {
    values: Vec<String>,
}

impl SecretRedactor {
    pub fn new(postgres: Option<&PostgresService>, s3: Option<&ObjectStoreService>) -> Self {
        let mut values = Vec::new();
        if let Some(pg) = postgres {
            values.push(pg.url.clone());
            if let Some(password) = pg
                .url
                .split("://")
                .nth(1)
                .and_then(|value| value.split('@').next())
                .and_then(|userinfo| userinfo.split_once(':'))
                .map(|(_, password)| password.to_owned())
                // A password can equal unavoidable public evidence vocabulary
                // (the local test service uses `fireweed`). In that case a raw
                // substring scan cannot distinguish a leak from the project
                // name; URL-authority and forbidden-field scans remain active.
                .filter(|password| {
                    !["fireweed", "pqueue", "postgres", "garage"]
                        .contains(&password.to_ascii_lowercase().as_str())
                })
            {
                values.push(password);
            }
        }
        if let Some(store) = s3 {
            values.extend([store.access.clone(), store.secret.clone()]);
        }
        values.retain(|value| value.len() >= 4);
        values.sort_by_key(|value| std::cmp::Reverse(value.len()));
        values.dedup();
        Self { values }
    }

    pub fn redact(&self, message: impl AsRef<str>) -> String {
        let mut safe = message.as_ref().to_owned();
        for value in &self.values {
            safe = safe.replace(value, "[REDACTED]");
        }
        redact_url_authorities(&safe)
    }

    pub fn validate_serialized_evidence(&self, bytes: &[u8]) -> Result<(), String> {
        let text = String::from_utf8_lossy(bytes);
        if self.values.iter().any(|value| text.contains(value)) {
            return Err("serialized evidence contains a configured credential value".into());
        }
        let lowercase = text.to_ascii_lowercase();
        for forbidden in [
            "password",
            "secret_access_key",
            "access_key_id",
            "postgres_url",
            "credential",
        ] {
            if lowercase.contains(&format!("\"{forbidden}\"")) {
                return Err(format!(
                    "serialized evidence contains forbidden field {forbidden}"
                ));
            }
        }
        Ok(())
    }
}

fn redact_url_authorities(message: &str) -> String {
    let mut output = String::with_capacity(message.len());
    let mut rest = message;
    while let Some(scheme) = rest.find("://") {
        let authority_start = scheme + 3;
        let authority_end = rest[authority_start..]
            .find(|c: char| c == '/' || c.is_whitespace())
            .map(|offset| authority_start + offset)
            .unwrap_or(rest.len());
        let authority = &rest[authority_start..authority_end];
        output.push_str(&rest[..authority_start]);
        if let Some(at) = authority.rfind('@') {
            output.push_str("[REDACTED]@");
            output.push_str(&authority[at + 1..]);
        } else {
            output.push_str(authority);
        }
        rest = &rest[authority_end..];
    }
    output.push_str(rest);
    output
}

pub fn derived_objectlog_schema(namespace: &str) -> String {
    let digest = Sha256::digest(namespace.as_bytes());
    format!("pq_{}", hex(&digest[..30]))
}

pub fn derived_plain_schema(namespace: &str) -> String {
    let digest = Sha256::digest(namespace.as_bytes());
    format!("fireweed_perf_{}", hex(&digest[..20]))
}

pub fn physical_object_prefix(namespace: &str) -> String {
    format!("{}/", hex(namespace.as_bytes()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Clone, Copy)]
pub enum SchemaKind {
    Plain,
    ObjectLog,
}

/// Unforgeable outside this module: all fields are private and validated.
pub struct AuthorizedCleanup {
    local: Option<PathBuf>,
    schema: Option<String>,
    namespace: Option<String>,
}

pub struct RunOwnership {
    run_id: String,
    canonical_root: PathBuf,
}

impl RunOwnership {
    pub fn new(work_root: &Path, run_id: &str) -> Result<Self, String> {
        validate_component(run_id, "run id")?;
        let run_root = work_root.join(run_id);
        std::fs::create_dir_all(&run_root).map_err(|error| format!("create run root: {error}"))?;
        let canonical_root = run_root
            .canonicalize()
            .map_err(|error| format!("canonicalize run root: {error}"))?;
        Ok(Self {
            run_id: run_id.to_owned(),
            canonical_root,
        })
    }

    pub fn authorize(
        &self,
        namespace: &str,
        local: Option<&Path>,
        schema_kind: Option<SchemaKind>,
        object_store: bool,
    ) -> Result<AuthorizedCleanup, String> {
        let parts = namespace.split('/').collect::<Vec<_>>();
        if parts.len() != 7
            || parts[0] != "fireweed-perf"
            || parts[1] != "v1"
            || parts[3] != self.run_id
            || !parts[2].bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err("cleanup namespace is not owned by this exact run".into());
        }
        for (label, value) in [
            ("commit", parts[2]),
            ("cell", parts[4]),
            ("shape", parts[5]),
            ("repetition", parts[6]),
        ] {
            validate_component(value, label)?;
        }
        let local = local
            .map(|path| {
                let canonical = path
                    .canonicalize()
                    .map_err(|error| format!("canonicalize cleanup path: {error}"))?;
                let expected = self
                    .canonical_root
                    .join(parts[4])
                    .join(parts[5])
                    .join(parts[6]);
                if canonical != expected || !canonical.starts_with(&self.canonical_root) {
                    return Err("local cleanup path is not the exact canonical run path".into());
                }
                Ok::<PathBuf, String>(canonical)
            })
            .transpose()?;
        let schema = schema_kind.map(|kind| match kind {
            SchemaKind::Plain => derived_plain_schema(namespace),
            SchemaKind::ObjectLog => derived_objectlog_schema(namespace),
        });
        Ok(AuthorizedCleanup {
            local,
            schema,
            namespace: object_store.then(|| namespace.to_owned()),
        })
    }
}

fn validate_component(value: &str, label: &str) -> Result<(), String> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(format!("invalid {label} component"));
    }
    Ok(())
}

pub fn cleanup_owned(
    cleanup: AuthorizedCleanup,
    postgres: Option<&PostgresService>,
    s3: Option<&ObjectStoreService>,
) -> Result<(), String> {
    if let Some(schema) = cleanup.schema {
        let pg = postgres.ok_or("PostgreSQL cleanup service missing")?;
        let mut client = Client::connect(&pg.url, NoTls)
            .map_err(|error| format!("PostgreSQL cleanup connection: {error}"))?;
        client
            .batch_execute(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
            .map_err(|error| format!("PostgreSQL schema cleanup: {error}"))?;
    }
    if let Some(namespace) = cleanup.namespace {
        let service = s3.ok_or("object-store cleanup service missing")?;
        let store = object_store(service)?;
        let prefix = physical_object_prefix(&namespace);
        let keys = store
            .list(&prefix)
            .map_err(|error| format!("object-store cleanup list: {error}"))?;
        if keys.iter().any(|key| !key.starts_with(&prefix)) {
            return Err("object-store returned a key outside the exact owned prefix".into());
        }
        for key in keys {
            store
                .delete(&key)
                .map_err(|error| format!("object-store cleanup delete: {error}"))?;
        }
        if !store
            .list(&prefix)
            .map_err(|error| format!("object-store cleanup verification: {error}"))?
            .is_empty()
        {
            return Err("object-store cleanup verification failed".into());
        }
    }
    if let Some(path) = cleanup.local {
        std::fs::remove_dir_all(path).map_err(|error| format!("local cleanup: {error}"))?;
    }
    Ok(())
}

fn object_store(config: &ObjectStoreService) -> Result<S3BlobStore, String> {
    S3BlobStore::new(
        &config.endpoint,
        &config.bucket,
        &config.access,
        &config.secret,
        &config.region,
    )
    .map_err(|error| format!("object-store client: {error}"))
}

pub fn object_store_preflight_rtts(config: &ObjectStoreService) -> Result<Vec<u64>, String> {
    let store = object_store(config)?;
    let mut samples = Vec::with_capacity(3);
    for _ in 0..3 {
        let started = std::time::Instant::now();
        store
            .list("fireweed-perf/v1/_locks/")
            .map_err(|error| format!("object-store preflight list: {error}"))?;
        samples.push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
    }
    Ok(samples)
}

/// Holds service locks for the lifetime of the run. The object lock is removed
/// only when its payload still matches this guard, so a stale guard cannot
/// delete a successor's lock.
pub struct ServiceLocks {
    postgres: Option<Client>,
    object_store: Option<(S3BlobStore, Vec<u8>)>,
}

impl ServiceLocks {
    pub fn acquire(
        postgres: Option<&PostgresService>,
        s3: Option<&ObjectStoreService>,
        run_id: &str,
        commit: &str,
    ) -> Result<Self, String> {
        let mut pg_lock = if let Some(service) = postgres {
            let mut client = Client::connect(&service.url, NoTls)
                .map_err(|error| format!("PostgreSQL lock connection: {error}"))?;
            let locked: bool = client
                .query_one("SELECT pg_try_advisory_lock(hashtext($1))", &[&LOCK_NAME])
                .map_err(|error| format!("PostgreSQL advisory lock: {error}"))?
                .get(0);
            if !locked {
                return Err("another matrix holds the PostgreSQL service lock".into());
            }
            Some(client)
        } else {
            None
        };
        let object_lock = if let Some(service) = s3 {
            let store = object_store(service)?;
            let payload = format!("run={run_id}\ncommit={commit}\n").into_bytes();
            match store.put_if_absent(LOCK_KEY, &payload) {
                Ok(true) => Some((store, payload)),
                Ok(false) => {
                    release_postgres(&mut pg_lock);
                    return Err("another matrix holds the object-store service lock".into());
                }
                Err(error) => {
                    release_postgres(&mut pg_lock);
                    return Err(format!("object-store service lock: {error}"));
                }
            }
        } else {
            None
        };
        Ok(Self {
            postgres: pg_lock,
            object_store: object_lock,
        })
    }
}

fn release_postgres(client: &mut Option<Client>) {
    if let Some(client) = client.as_mut() {
        let _ = client.query("SELECT pg_advisory_unlock(hashtext($1))", &[&LOCK_NAME]);
    }
}

impl Drop for ServiceLocks {
    fn drop(&mut self) {
        release_postgres(&mut self.postgres);
        if let Some((store, payload)) = self.object_store.as_ref()
            && store.get(LOCK_KEY).ok().flatten().as_deref() == Some(payload.as_slice())
        {
            let _ = store.delete(LOCK_KEY);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_configured_values_and_url_userinfo() {
        let pg = PostgresService {
            url: "postgres://alice:hunter2@db.invalid/fireweed".into(),
        };
        let s3 = ObjectStoreService {
            endpoint: "http://garage.invalid".into(),
            bucket: "bench".into(),
            region: "garage".into(),
            access: "access-key".into(),
            secret: "secret-key".into(),
        };
        let safe = SecretRedactor::new(Some(&pg), Some(&s3)).redact(format!(
            "{} access-key secret-key https://bob:password@host/path hunter2",
            pg.url
        ));
        assert!(!safe.contains("hunter2"));
        assert!(!safe.contains("access-key"));
        assert!(!safe.contains("secret-key"));
        assert!(!safe.contains("password"));
        assert!(
            redactor_fixture()
                .validate_serialized_evidence(b"{\"status\":\"passed\"}")
                .is_ok()
        );
        assert!(
            redactor_fixture()
                .validate_serialized_evidence(b"{\"password\":\"x\"}")
                .is_err()
        );
        assert!(
            redactor_fixture()
                .validate_serialized_evidence(b"secret-key")
                .is_err()
        );
        assert!(
            redactor_fixture()
                .validate_serialized_evidence(b"fireweed alice")
                .is_ok()
        );
    }

    fn redactor_fixture() -> SecretRedactor {
        SecretRedactor::new(
            Some(&PostgresService {
                url: "postgres://alice:hunter2@db.invalid/fireweed".into(),
            }),
            Some(&ObjectStoreService {
                endpoint: "http://garage.invalid".into(),
                bucket: "bench".into(),
                region: "garage".into(),
                access: "access-key".into(),
                secret: "secret-key".into(),
            }),
        )
    }

    #[test]
    fn authorization_requires_exact_namespace_and_canonical_path() {
        let base = std::env::temp_dir().join(format!(
            "fireweed-services-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let ownership = RunOwnership::new(&base, "run-1").unwrap();
        let path = base.join("run-1/memory/minimal/r00");
        std::fs::create_dir_all(&path).unwrap();
        let namespace = "fireweed-perf/v1/abcdef/run-1/memory/minimal/r00";
        assert!(
            ownership
                .authorize(
                    "fireweed-perf/v1/abcdef/other/memory/minimal/r00",
                    Some(&path),
                    None,
                    false
                )
                .is_err()
        );
        assert!(
            ownership
                .authorize(namespace, Some(&base), None, false)
                .is_err()
        );
        let cleanup = ownership
            .authorize(namespace, Some(&path), None, false)
            .unwrap();
        cleanup_owned(cleanup, None, None).unwrap();
        assert!(!path.exists());
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn derivations_match_facade_contract() {
        let namespace = "fireweed-perf/v1/abcdef/run-1/memory/minimal/r00";
        assert_eq!(derived_objectlog_schema(namespace).len(), 63);
        assert_eq!(
            physical_object_prefix(namespace),
            format!("{}/", hex(namespace.as_bytes()))
        );
    }
}
