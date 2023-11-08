use color_eyre::Result;
use hopsworks_rs::{
    clients::rest_client::HopsworksClientBuilder,
    domain::{query::controller::read_query_from_online_feature_store, storage_connector},
    hopsworks_login,
};
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
        .get_feature_group_by_name_and_version(fg_name.as_str(), 1)
        .await?
    {
        let query = feature_group.select(
            feature_group
                .get_feature_names()
                .iter()
                .map(|s| s.as_ref())
                .collect(),
        )?;
        let online_storage_connector =
            storage_connector::controller::get_feature_store_online_connector(
                feature_group.featurestore_id,
            )
            .await?;
        let now = Instant::now();
        let read_df = read_query_from_online_feature_store(query, online_storage_connector).await?;
        println!(
            "Read feature group took {:.2?} seconds and returned :\n{:#?}",
            now.elapsed(),
            read_df
        );
    }

    Ok(())
}
