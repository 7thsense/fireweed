use std::sync::Arc;

use fireweed::{NewItem, open_memory};
use fireweed_bench::{SystemClock, all_shapes, bench_qdef, qkey};

#[test]
fn resident_terminal_count_tracks_terminal_population_via_metrics() {
    let shape = all_shapes()[0];
    let fireweed = open_memory(Arc::new(SystemClock));
    let queue = qkey("terminal-metrics");

    futures::executor::block_on(async {
        fireweed.create_queue(bench_qdef("bench", "terminal-metrics", &shape))
            .await
            .unwrap();

        fireweed.push_batch(&queue, vec![NewItem::default(), NewItem::default()])
            .await
            .unwrap();

        let claimed = fireweed.claim(&queue, 2, 3_600_000).await.unwrap();
        assert_eq!(claimed.len(), 2);

        fireweed.ack(&queue, vec![claimed[0].item_id]).await.unwrap();
        fireweed.fail(&queue, vec![claimed[1].item_id]).await.unwrap();

        let metrics = fireweed.metrics(&queue).await.unwrap();
        assert_eq!((metrics.pending, metrics.leased), (0, 0));
        assert_eq!((metrics.complete, metrics.failed), (1, 1));
        assert_eq!(metrics.resident_terminal_count, 2);
    });
}
