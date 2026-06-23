use std::path::Path;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tokio_postgres::{Client, NoTls};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalPostgresProfile {
    pub backend_profile: String,
    pub database_url: String,
    pub docker_container_name: Option<String>,
}

pub struct LocalPostgresConnection {
    pub client: Arc<Mutex<Client>>,
    connection_task: tokio::task::JoinHandle<()>,
}

impl Drop for LocalPostgresConnection {
    fn drop(&mut self) {
        self.connection_task.abort();
    }
}

impl LocalPostgresProfile {
    pub fn from_fixture(path: impl AsRef<Path>) -> Self {
        let text = std::fs::read_to_string(path).expect("profile fixture should be readable");
        let backend_profile = parse_string_field(&text, "backend_profile");
        let database_url = parse_string_field(&text, "database_url");
        let docker_container_name = parse_optional_string_field(&text, "docker_container_name");
        Self {
            backend_profile,
            database_url,
            docker_container_name,
        }
    }

    pub async fn connect(&self) -> LocalPostgresConnection {
        assert_eq!(self.backend_profile, "postgres_native");
        let mut database_urls = vec![self.database_url.clone()];
        if let Some(container_name) = &self.docker_container_name
            && let Some(container_ip) = docker_container_ip(container_name)
        {
            database_urls.push(format!(
                "host={container_ip} port=5432 user=pqueue password=pqueue dbname=pqueue"
            ));
        }
        let (client, connection) = connect_with_retry(&database_urls).await;
        let connection_task = tokio::spawn(async move {
            if let Err(err) = connection.await {
                eprintln!("postgres connection ended: {err}");
            }
        });
        LocalPostgresConnection {
            client: Arc::new(Mutex::new(client)),
            connection_task,
        }
    }
}

async fn connect_with_retry(
    database_urls: &[String],
) -> (
    Client,
    tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
) {
    let mut last_error = None;
    for database_url in database_urls {
        for _ in 0..60 {
            match tokio_postgres::connect(database_url, NoTls).await {
                Ok(connection) => return connection,
                Err(err) => {
                    last_error = Some(err);
                    sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }
    panic!(
        "local postgres_native database should be reachable: {}",
        last_error
            .map(|err| err.to_string())
            .unwrap_or_else(|| "connection was not attempted".to_string())
    );
}

fn docker_container_ip(container_name: &str) -> Option<String> {
    let output = std::process::Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
            container_name,
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let ip = String::from_utf8(output.stdout).ok()?.trim().to_string();
    (!ip.is_empty()).then_some(ip)
}

fn parse_string_field(text: &str, field: &str) -> String {
    parse_optional_string_field(text, field)
        .unwrap_or_else(|| panic!("missing string field `{field}`"))
}

fn parse_optional_string_field(text: &str, field: &str) -> Option<String> {
    let prefix = format!("{field} = ");
    text.lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|raw| raw.strip_prefix('"')?.strip_suffix('"'))
        .map(str::to_string)
}
