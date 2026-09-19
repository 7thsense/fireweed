use std::sync::Arc;

fn main() {
    let queue = fireweed::open_product(Arc::new(fireweed::SystemClock));
    let _ = queue.backend();
}
