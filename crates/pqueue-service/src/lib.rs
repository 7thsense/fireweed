#![forbid(unsafe_code)]

pub mod scaffold {
    pub fn client_name() -> &'static str {
        pqueue_client::scaffold::core_name()
    }

    pub fn core_name() -> &'static str {
        pqueue_core::scaffold::name()
    }

    pub fn postgres_core_name() -> &'static str {
        pqueue_postgres::scaffold::core_name()
    }

    pub fn storage_name() -> &'static str {
        pqueue_storage::scaffold::core_name()
    }
}
