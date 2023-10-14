use color_eyre::Result;
use hopsworks_rs::{clients::rest_client::HopsworksClientBuilder, hopsworks_login};

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    env_logger::init();

    let project = hopsworks_login(Some(
        HopsworksClientBuilder::default()
            .with_url(std::env::var("HOPSWORKS_URL").unwrap_or_default().as_str()),
    ))
    .await
    .expect("Error connecting to Hopsworks:\n");

    let feature_store = project.get_feature_store().await?;

    let feature_group = feature_store
        .get_feature_group_by_name_and_version("transactions_fraud_batch_fg_2_rust", 1)
        .await?;

    let read_df = feature_group.read_with_arrow_flight_client();

    Ok(())
}
