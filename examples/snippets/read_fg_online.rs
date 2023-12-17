use color_eyre::Result;
use hopsworks_rs::{hopsworks_login, HopsworksClientBuilder};
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    color_eyre::install()?;
    env_logger::init();

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
        let query = feature_group.select(
            feature_group
                .get_feature_names()
                .iter()
                .map(|s| s.as_ref())
                .collect(),
        )?;
        let now = Instant::now();
        let read_df = query.read_from_online_feature_store().await?;
        println!(
            "Read feature group took {:.2?} seconds and returned :\n{:#?}",
            now.elapsed(),
            read_df
        );
    }

    Ok(())
}
