#![forbid(unsafe_code)]

pub mod control_plane;
pub mod schema;

mod convert;

pub use control_plane::PostgresControlPlaneStore;

pub mod scaffold {
    pub fn core_name() -> &'static str {
        pqueue_core::scaffold::name()
    }

    pub fn storage_name() -> &'static str {
        pqueue_storage::scaffold::core_name()
    }
}
