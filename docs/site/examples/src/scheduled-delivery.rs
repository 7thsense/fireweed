// Provenance: crates/fireweed/examples/queue_lifecycle.rs::retry_and_schedule
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
async fn retry_and_schedule(queue: &Queue, address: &QueueAddress) -> QueueResult<()> {
    let later = now() + 3_600_000_000_000;
    queue
        .insert(InsertRequest {
            queue: address.clone(),
            request_id: "load-2".into(),
            mode: InsertMode::InsertOnly,
            items: vec![job("flaky", 1, 0), job("nightly", 1, later)],
            issued_at: now(),
        })
        .await?;

    // `nightly` is not eligible for an hour, so only `flaky` is delivered.
    let claimed = queue
        .claim(claim_request(address, "claim-2", now()))
        .await?;
    assert_eq!(claimed.len(), 1);
    let retry_at = now() + 60_000_000_000;
    queue
        .commit(settle(
            address,
            "retry-1",
            &claimed[0],
            Disposition::RetryAt(retry_at),
        ))
        .await?;

    // Neither item is eligible yet; both become claimable once the clock passes their times.
    assert!(
        queue
            .claim(claim_request(address, "claim-3", now()))
            .await?
            .is_empty()
    );
    let due = queue
        .claim(claim_request(address, "claim-4", later + 1))
        .await?;
    assert_eq!(due.len(), 2);
    Ok(())
}
