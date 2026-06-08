#![forbid(unsafe_code)]

pub mod scaffold {
    pub fn core_name() -> &'static str {
        pqueue_core::scaffold::name()
    }
}
