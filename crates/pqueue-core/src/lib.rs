#![forbid(unsafe_code)]

mod domain;

pub mod scaffold {
    pub const NAME: &str = "pqueue-core";

    pub fn name() -> &'static str {
        NAME
    }
}

pub use domain::*;
