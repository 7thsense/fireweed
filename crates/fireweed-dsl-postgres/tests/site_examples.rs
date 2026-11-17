//! Runs the microsite's example documents (docs/site/examples/src) against
//! PostgreSQL and checks the results their pages describe. The documents are
//! included unchanged; only the ids and tokens a claim returns, and a version
//! read from the row, are filled in as params. Needs `FIREWEED_PG_TEST_URL`.

use fireweed_dsl::{Program, RejectReason, Response, Schema, Status, from_yaml};
use fireweed_dsl_postgres::{apply_schema, execute};
use serde_json::{Value, json};
use tokio_postgres::{Client, NoTls};

const SCHEMA: &str = include_str!("../../../docs/site/examples/src/schema.yaml");
const ENQUEUE: &str =
    include_str!("../../../docs/site/examples/src/queue-lifecycle/1-enqueue.yaml");
const CLAIM: &str = include_str!("../../../docs/site/examples/src/queue-lifecycle/2-claim.yaml");
const SETTLE: &str = include_str!("../../../docs/site/examples/src/queue-lifecycle/3-settle.yaml");
const CREATE_CHUNK: &str =
    include_str!("../../../docs/site/examples/src/guarded-update/1-create-chunk.yaml");
const ADVANCE_CURSOR: &str =
    include_str!("../../../docs/site/examples/src/guarded-update/2-advance-cursor.yaml");
const GUARDED_SEND: &str =
    include_str!("../../../docs/site/examples/src/guarded-update/3-guarded-send.yaml");
const COUNTS: &str = include_str!("../../../docs/site/examples/src/indexed-count/1-counts.yaml");

const TENANT: &str = "site";

fn doc(text: &str) -> Program {
    from_yaml(text).unwrap()
}

/// Runs `p` with the database clock at its `issued_at`, as if sent then.
async fn run(c: &mut Client, sp: &str, p: &Program) -> Response {
    let at = p.issued_at.as_deref().unwrap_or("2026-11-16T09:32:00Z");
    c.batch_execute(&format!("SET fireweed.now = '{at}'"))
        .await
        .unwrap();
    execute(c, TENANT, sp, p)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{p:?}"))
}

fn rows(r: &Response, i: usize) -> Vec<Value> {
    r.results[i].as_array().cloned().unwrap_or_default()
}

fn rejected(r: &Response) -> Option<(usize, RejectReason)> {
    r.rejected.as_ref().map(|x| (x.statement, x.reason))
}

/// The same program under another request id: a second sender.
fn again(mut p: Program, id: &str) -> Program {
    p.request_id = Some(id.into());
    p
}

#[tokio::test]
async fn site_examples_run_as_their_pages_describe() {
    let Ok(url) = std::env::var("FIREWEED_PG_TEST_URL") else {
        return;
    };
    let (mut c, conn) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(conn);
    let sp = format!(
        "examples-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema: Schema = from_yaml(SCHEMA).unwrap();
    apply_schema(&mut c, TENANT, &sp, &schema, None)
        .await
        .unwrap();

    // Enqueue, claim, settle.
    let enqueue = doc(ENQUEUE);
    let r = run(&mut c, &sp, &enqueue).await;
    assert_eq!(r.status, Status::Committed);
    assert!(
        rows(&r, 0).iter().all(|x| x["status"] == "inserted"),
        "{r:?}"
    );
    let r = run(&mut c, &sp, &enqueue).await;
    assert!(r.replayed, "sending the same program again replays it");

    let r = run(&mut c, &sp, &doc(CLAIM)).await;
    let claimed = rows(&r, 0);
    let keys: Vec<&Value> = claimed.iter().map(|x| &x["key"]).collect();
    assert_eq!(keys, [&json!("c7-ana"), &json!("c7-bo")], "{claimed:?}");
    assert!(claimed.iter().all(|x| x["attempts"] == 1));

    let mut settle = doc(SETTLE);
    for (name, item) in [("ana", &claimed[0]), ("bo", &claimed[1])] {
        settle.params.insert(name.into(), item["id"].clone());
        settle
            .params
            .insert(format!("{name}_token"), item["token"].clone());
    }
    let r = run(&mut c, &sp, &settle).await;
    assert_eq!(r.status, Status::Committed, "{r:?}");
    for i in 0..2 {
        assert!(rows(&r, i).iter().all(|x| x["status"] == "ok"), "{r:?}");
    }

    // Guarded updates.
    let create = doc(CREATE_CHUNK);
    assert_eq!(run(&mut c, &sp, &create).await.status, Status::Committed);
    let r = run(&mut c, &sp, &again(create, "r1-chunk-3-second-admitter")).await;
    assert_eq!(rejected(&r), Some((0, RejectReason::KeyConflict)));

    // The page's version is illustrative; read the row's real one.
    let r = execute(
        &mut c,
        TENANT,
        &sp,
        &doc("statements:\n  - select: {from: chunks, where: {and: [{eq: [run, r1]}, {eq: [chunk, 3]}]}, fields: [version], limit: 1}\n"),
    )
    .await
    .unwrap();
    let version = rows(&r, 0)[0]["version"].clone();
    let mut advance = doc(ADVANCE_CURSOR);
    advance.params.insert("seen_version".into(), version);
    let r = run(&mut c, &sp, &advance).await;
    assert_eq!(r.results[0], json!({"matched": 1, "met": true}), "{r:?}");
    // The version moved, so the same compare-and-set from another writer fails.
    let r = run(&mut c, &sp, &again(advance, "r1-chunk-3-advance-stale")).await;
    assert_eq!(rejected(&r), Some((0, RejectReason::ExpectUnmet)));

    let send = doc(GUARDED_SEND);
    assert_eq!(run(&mut c, &sp, &send).await.status, Status::Committed);
    run(
        &mut c,
        &sp,
        &doc("request_id: suppress-dee\nissued_at: 2026-11-16T09:31:20Z\nstatements:\n  - insert: {into: suppression, rows: [{address: dee@example.com, reasons: [complaint]}]}\n"),
    )
    .await;
    let r = run(&mut c, &sp, &again(send, "c7-enqueue-dee-again")).await;
    assert_eq!(rejected(&r), Some((0, RejectReason::ExpectUnmet)));

    // Indexed counts after the examples above.
    let r = run(&mut c, &sp, &doc(COUNTS)).await;
    let by_state = json!([{"state": "complete", "n": 1}, {"state": "pending", "n": 3}]);
    assert_eq!(r.results[0], by_state, "{r:?}");
    assert_eq!(r.results[1], json!([{"campaign": "c7", "n": 4}]));
    assert_eq!(r.results[2], by_state);
}
