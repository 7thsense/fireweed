//! Production container entrypoint for the pqueue API-001 service.
//!
//! The binary parses runtime configuration from the environment (see
//! `pqueue_service::runtime` and `docs/deployment/container-runtime-contract.md`),
//! binds the configured listen address, and serves the API-001 app plus the
//! liveness/readiness health probes until terminated.

use std::process::ExitCode;

use pqueue_service::runtime::{RuntimeConfig, help_text};

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Some(flag) = args.first() {
        match flag.as_str() {
            "-h" | "--help" => {
                print!("{}", help_text());
                return ExitCode::SUCCESS;
            }
            "-V" | "--version" => {
                println!("pqueue-service {}", env!("CARGO_PKG_VERSION"));
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("error: unrecognized argument `{other}`\n");
                eprint!("{}", help_text());
                return ExitCode::FAILURE;
            }
        }
    }

    let config = match RuntimeConfig::from_env() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("error: invalid runtime configuration: {err}");
            return ExitCode::FAILURE;
        }
    };

    match serve(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: pqueue-service failed: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn serve(config: RuntimeConfig) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    let local_addr = listener.local_addr()?;
    eprintln!(
        "pqueue-service listening on {local_addr} (backend_profile={}, principal_id={}, tenants={})",
        config.backend_profile.as_str(),
        config.principal_id,
        config.tenants.len(),
    );
    axum::serve(listener, config.router())
        .with_graceful_shutdown(shutdown_signal())
        .await
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
