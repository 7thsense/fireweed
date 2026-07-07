use std::sync::Arc;

use pqueue::{NewItem, Pqueue};
use pqueue_bench::{SystemClock, all_shapes, bench_qdef, qkey};
use pqueue_memory::composed_memory_backend;

#[test]
fn resident_terminal_count_tracks_terminal_population_via_metrics() {
    let shape = all_shapes()[0];
    let pq = Pqueue::new(Arc::new(composed_memory_backend()), Arc::new(SystemClock));
    let queue = qkey("terminal-metrics");

    futures::executor::block_on(async {
        pq.create_queue(bench_qdef("bench", "terminal-metrics", &shape))
            .await
            .unwrap();

        pq.push_batch(&queue, vec![NewItem::default(), NewItem::default()])
            .await
            .unwrap();

        let claimed = pq.claim(&queue, 2, 3_600_000).await.unwrap();
        assert_eq!(claimed.len(), 2);

        pq.ack(&queue, vec![claimed[0].item_id]).await.unwrap();
        pq.fail(&queue, vec![claimed[1].item_id]).await.unwrap();

        let metrics = pq.metrics(&queue).await.unwrap();
        assert_eq!((metrics.pending, metrics.leased), (0, 0));
        assert_eq!((metrics.complete, metrics.failed), (1, 1));
        assert_eq!(metrics.resident_terminal_count, 2);
    });
}
