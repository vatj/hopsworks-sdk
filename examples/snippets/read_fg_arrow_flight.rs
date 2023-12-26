use anyhow::Context;
use color_eyre::Result;
use hopsworks_rs::{hopsworks_login, HopsworksClientBuilder};
use std::time::Instant;

fn setup_tracing_logging() -> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::{util::SubscriberInitExt, EnvFilter, FmtSubscriber};
    tracing_log::LogTracer::init().context("tracing log init")?;

    let filter = std::env::var("TRACING_LOG").unwrap_or_else(|_| "debug".to_string());
    let filter = EnvFilter::try_new(filter).context("set up log env filter")?;

    let subscriber = FmtSubscriber::builder().with_env_filter(filter).finish();
    subscriber.try_init().context("init logging subscriber")?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_tracing_logging()?;

    let project = hopsworks_login(Some(
        HopsworksClientBuilder::default()
            .with_url(std::env::var("HOPSWORKS_URL").unwrap_or_default().as_str()),
    ))
    .await
    .expect("Error connecting to Hopsworks:\n");

    let feature_store = project.get_feature_store().await?;

    let fg_name = std::env::var("HOPSWORKS_FEATURE_GROUP_NAME").unwrap_or_default();

    if let Some(feature_group) = feature_store
        .get_feature_group(fg_name.as_str(), Some(1))
        .await?
    {
        let now = Instant::now();
        let read_df = feature_group.read_from_offline_feature_store().await?;
        println!(
            "Read feature group took {:.2?} seconds and returned :\n{:#?}",
            now.elapsed(),
            read_df.head(Some(10))
        );
    }

    Ok(())
}
