//! Live TLS-connection proof for the postgres adapter (the Lakebase / cloud-postgres transport).
//!
//! Behind the `tls` cargo feature AND env-gated on `PQUEUE_PG_TLS_TEST_URL` (a DSN with `sslmode=require`
//! pointing at a TLS-enabled postgres). LOUD-skips otherwise so a CI/dev machine without a TLS database
//! never reports a false pass. The companion harness `scripts/dev/pg-tls-smoke.sh` starts a self-signed
//! `postgres:16 -c ssl=on` in docker and runs exactly this test end-to-end.
#![cfg(feature = "tls")]

use pqueue_postgres::{PostgresConnectConfig, PostgresSslMode, connect};

/// Connect over TLS to a `sslmode=require` postgres and do a real round-trip, asserting the backend
/// confirms the session is encrypted (`pg_stat_ssl.ssl = true`). This exercises the production
/// `connect()` path (native-tls connector selection + handshake), not a bespoke connector.
#[test]
fn postgres_tls_connection_succeeds() {
    let Ok(url) = std::env::var("PQUEUE_PG_TLS_TEST_URL") else {
        eprintln!(
            "POSTGRES TLS ROUND-TRIP SKIPPED — set PQUEUE_PG_TLS_TEST_URL to a TLS-enabled postgres DSN \
             (sslmode=require) to run this proof"
        );
        return;
    };

    // The DSN must actually demand TLS; a misconfigured `disable` URL here would silently prove nothing.
    let ssl_mode = PostgresConnectConfig::new(&url)
        .parsed_ssl_mode()
        .expect("PQUEUE_PG_TLS_TEST_URL parses");
    assert!(
        matches!(ssl_mode, PostgresSslMode::Require | PostgresSslMode::Prefer),
        "PQUEUE_PG_TLS_TEST_URL must request TLS (sslmode=require|prefer), got {ssl_mode:?}"
    );

    let mut client = connect(PostgresConnectConfig::new(&url))
        .expect("TLS connect to the live database succeeds");

    // Round-trip 1: a trivial query proves the encrypted session carries real traffic.
    let answer: i32 = client
        .query_one("SELECT 42", &[])
        .expect("SELECT round-trip over TLS")
        .get(0);
    assert_eq!(answer, 42, "TLS round-trip returns the expected value");

    // Round-trip 2: the server confirms THIS backend session is TLS-encrypted (not a plaintext downgrade).
    let ssl_active: bool = client
        .query_one(
            "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
            &[],
        )
        .expect("pg_stat_ssl round-trip")
        .get(0);
    assert!(
        ssl_active,
        "pg_stat_ssl must report the session as TLS-encrypted; a false here means the connection \
         silently downgraded to plaintext"
    );

    // Round-trip 3: an actual write/read through a temp table, so the proof is not read-only.
    client
        .batch_execute(
            "CREATE TEMP TABLE pqueue_tls_probe (v int); INSERT INTO pqueue_tls_probe VALUES (7);",
        )
        .expect("temp-table write over TLS");
    let stored: i32 = client
        .query_one("SELECT v FROM pqueue_tls_probe", &[])
        .expect("temp-table read over TLS")
        .get(0);
    assert_eq!(stored, 7, "value written over TLS reads back");
}
