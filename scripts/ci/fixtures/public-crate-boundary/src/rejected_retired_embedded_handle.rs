use fireweed::EmbeddedHandle;

fn name(value: Option<EmbeddedHandle>) {
    drop(value);
}

fn main() {}
