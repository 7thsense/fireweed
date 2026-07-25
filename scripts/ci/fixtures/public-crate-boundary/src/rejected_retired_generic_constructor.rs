use std::sync::Arc;

use fireweed::{Clock, LibBackend, Pqueue};

fn construct<B: LibBackend>(backend: Arc<B>, clock: Arc<dyn Clock>) -> Pqueue<B> {
    Pqueue::new(backend, clock)
}

fn main() {}
