// Provenance: crates/fireweed-resp/tests/e2e.rs::drain_and_reconcile_with_offtheshelf_client
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
async fn drain_and_reconcile_with_offtheshelf_client() {
    let backend = Arc::new(composed_memory_backend());
    backend.create_queue(qdef()).await.unwrap();
    let (mut con, _) = serve_backend(backend.clone(), Arc::new(SystemClock)).await;

    // Produce mixed priorities incl. a DUPLICATE (two 30s) to exercise the CreatedSequence
    // tie-breaker. Record the server-assigned id per insertion so we can check tie order.
    let priorities: Vec<i64> = vec![50, 10, 90, 30, 70, 20, 80, 40, 60, 5, 30];
    let mut produced_ids: Vec<String> = Vec::new();
    for &p in &priorities {
        let id: String = redis::cmd("XADD")
            .arg("t1:q1")
            .arg("*")
            .arg("priority")
            .arg(p)
            .query_async(&mut con)
            .await
            .unwrap();
        produced_ids.push(id);
    }

    // Drain COUNT 3 at a time, acking each batch, until empty.
    let mut delivered: Vec<(i64, String)> = Vec::new();
    let mut round_bounds: Vec<(i64, i64)> = Vec::new(); // (min, max) priority per round
    let mut rounds = 0;
    loop {
        rounds += 1;
        assert!(
            rounds < 100,
            "drain did not terminate (possible orphan/hang)"
        );
        let reply: Option<StreamReadReply> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("g")
            .arg("c")
            .arg("COUNT")
            .arg(3)
            .arg("STREAMS")
            .arg("t1:q1")
            .arg(">")
            .query_async(&mut con)
            .await
            .unwrap();
        let Some(reply) = reply else { break };
        if reply.keys.iter().all(|k| k.ids.is_empty()) {
            break;
        }
        let mut round: Vec<i64> = Vec::new();
        let mut ack = redis::cmd("XACK");
        ack.arg("t1:q1").arg("g");
        for key in &reply.keys {
            for id in &key.ids {
                let p: i64 = id.get("priority").expect("priority field present");
                delivered.push((p, id.id.clone()));
                round.push(p);
                ack.arg(&id.id);
            }
        }
        round_bounds.push((*round.iter().min().unwrap(), *round.iter().max().unwrap()));
        let _acked: i64 = ack.query_async(&mut con).await.unwrap();
    }

    // (a) exactly-once + global priority order: delivered priorities == sorted(produced).
    let delivered_pri: Vec<i64> = delivered.iter().map(|(p, _)| *p).collect();
    let mut expected = priorities.clone();
    expected.sort();
    assert_eq!(
        delivered_pri, expected,
        "delivered set must equal produced set, in priority order (Invariant 1, exactly once)"
    );

    // (b) cross-batch ordering: each round's max <= the next round's min (so a backend that sorted
    // only WITHIN a batch would fail here).
    for w in round_bounds.windows(2) {
        assert!(
            w[0].1 <= w[1].0,
            "round priority bands must not overlap: {:?} then {:?}",
            w[0],
            w[1]
        );
    }

    // (c) tie-break: the first-inserted priority-30 item is delivered before the second.
    let first_30 = &produced_ids[3];
    let second_30 = &produced_ids[10];
    let pos_first = delivered.iter().position(|(_, id)| id == first_30).unwrap();
    let pos_second = delivered
        .iter()
        .position(|(_, id)| id == second_30)
        .unwrap();
    assert!(
        pos_first < pos_second,
        "equal-priority items must break ties by insertion order (CreatedSequence)"
    );
}
