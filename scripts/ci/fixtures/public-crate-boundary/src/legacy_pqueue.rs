use fireweed::Pqueue;

fn name<B>(value: Option<Pqueue<B>>) {
    drop(value);
}

fn main() {}
