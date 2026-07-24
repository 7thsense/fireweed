use std::sync::Arc;

fn main() {
    let queue = fireweed::open_memory(Arc::new(fireweed::SystemClock));
    let _ = queue;
}
