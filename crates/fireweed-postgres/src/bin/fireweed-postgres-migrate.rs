use fireweed_postgres::PostgresRelationalBackend;

fn value_after(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == flag)
        .map(|pair| pair[1].clone())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let url = value_after(&args, "--url")
        .or_else(|| std::env::var("FIREWEED_PG_URL").ok())
        .or_else(|| std::env::var("PQUEUE_PG_URL").ok())
        .unwrap_or_else(|| {
            eprintln!("usage: fireweed-postgres-migrate --url URL [--schema NAME] [--batch-size N] [--max-batches N]");
            std::process::exit(2);
        });
    let schema = value_after(&args, "--schema");
    let batch_size = value_after(&args, "--batch-size")
        .map(|value| {
            value
                .parse::<u32>()
                .expect("--batch-size must be an integer")
        })
        .unwrap_or(10_000);
    let max_batches = value_after(&args, "--max-batches")
        .map(|value| {
            value
                .parse::<u64>()
                .expect("--max-batches must be an integer")
        })
        .unwrap_or(u64::MAX);

    if let Err(error) = match &schema {
        Some(schema) => {
            PostgresRelationalBackend::apply_concurrent_migrations_in_schema(&url, schema)
        }
        None => PostgresRelationalBackend::apply_concurrent_migrations(&url),
    } {
        eprintln!("concurrent index migration failed: {error}");
        std::process::exit(1);
    }

    for _ in 0..max_batches {
        let result = match &schema {
            Some(schema) => {
                PostgresRelationalBackend::migrate_metrics_batch_in_schema(&url, schema, batch_size)
            }
            None => PostgresRelationalBackend::migrate_metrics_batch(&url, batch_size),
        };
        match result {
            Ok(progress) => {
                println!(
                    "metrics_migration rows_processed={} rows_backfilled={} due_rows_backfilled={} batches_completed={} complete={}",
                    progress.rows_processed,
                    progress.rows_backfilled,
                    progress.due_rows_backfilled,
                    progress.batches_completed,
                    progress.complete
                );
                if progress.complete {
                    return;
                }
            }
            Err(error) => {
                eprintln!("metrics migration failed: {error}");
                std::process::exit(1);
            }
        }
    }
    eprintln!("metrics migration is incomplete after the configured --max-batches={max_batches}");
    std::process::exit(3);
}
