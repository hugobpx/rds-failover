mod pg;

use pg::Pg;
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Database connection
    let primary_url = std::env::var("PRIMARY_URL").expect("PRIMARY_URL must be set");
    let replica_url = std::env::var("REPLICA_URL");

    info!("Connecting to: {primary_url}");

    // Create database connection
    let primary_instance = Arc::new(Pg::new(&primary_url).await?);
    let replica_instance = Arc::new(Pg::new(&replica_url.unwrap_or(primary_url)).await?);

    // Run migrations
    info!("Running migrations");
    primary_instance.migrate().await?;

    // Printer
    let primary_latest = Arc::new(AtomicU64::new(0));
    let secondary_latest = Arc::new(AtomicU64::new(0));
    tokio::spawn({
        let primary_latest = primary_latest.clone();
        let secondary_latest = secondary_latest.clone();
        async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            loop {
                let primary = primary_latest.load(Ordering::Relaxed);
                let secondary = secondary_latest.load(Ordering::Relaxed);
                info!(
                    "{primary} - {secondary} - Lag: {}",
                    Duration::from_millis(primary)
                        .abs_diff(Duration::from_millis(secondary))
                        .as_millis()
                );
                interval.tick().await;
            }
        }
    });

    // Read
    let reader = |pg: Arc<Pg>, latest: Arc<AtomicU64>, name: &'static str| {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        let pg = pg.clone();
        let tracker = latest.clone();
        async move {
            info!("{name} reader started");

            loop {
                match pg.read_latest().await {
                    Ok(latest) => {
                        if let Some((_id, value)) = latest {
                            tracker.store(value as u64, Ordering::Relaxed);
                        }
                    }
                    Err(e) => error!(%e, "{name} reading error"),
                }

                interval.tick().await;
            }
        }
    };

    tokio::spawn(reader(primary_instance.clone(), primary_latest, "Primary"));
    tokio::spawn(reader(replica_instance, secondary_latest, "Secondary"));

    // Write
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        if let Err(e) = primary_instance
            .write(chrono::Utc::now().timestamp_millis())
            .await
        {
            error!(%e, "Writing error");
        }
        interval.tick().await;
    }
}
