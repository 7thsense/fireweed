#![forbid(unsafe_code)]

pub mod append;
pub mod connect;
pub mod control_plane;
pub mod retention;
pub mod schema;

mod convert;

pub use append::PostgresAppendStore;
pub use control_plane::PostgresControlPlaneStore;

pub mod scaffold {
    pub fn core_name() -> &'static str {
        pqueue_core::scaffold::name()
    }

    pub fn storage_name() -> &'static str {
        pqueue_storage::scaffold::core_name()
    }
}
