use fireweed_dsl::{Program, Schema, from_yaml};
use fireweed_dsl_postgres::{apply_schema, execute};
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")?;
    let (mut client, connection) = tokio_postgres::connect(&url, NoTls).await?;
    tokio::spawn(connection);

    // Create the space, or leave it as it is if the schema is unchanged.
    let schema: Schema = from_yaml(&std::fs::read_to_string("schema.yaml")?)?;
    apply_schema(&mut client, "acme", "campaign-c7", &schema, None).await?;

    // Run one program as one transaction.
    let program: Program = from_yaml(&std::fs::read_to_string("program.yaml")?)?;
    let response = execute(&mut client, "acme", "campaign-c7", &program).await?;
    println!("{}", serde_json::to_string_pretty(&response)?);
    Ok(())
}
