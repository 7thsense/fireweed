use fireweed::EmbeddedPqueue;

fn name<B>(value: Option<EmbeddedPqueue<B>>) {
    drop(value);
}

fn main() {}
