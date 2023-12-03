use color_eyre::Result;
use hopsworks_rs::HopsworksClientBuilder;

use hopsworks_rs::hopsworks_login;
use polars::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let project = hopsworks_login(Some(
        HopsworksClientBuilder::default()
            .with_url(std::env::var("HOPSWORKS_URL").unwrap_or_default().as_str()),
    ))
    .await
    .expect("Error connecting to Hopsworks:\n");

    let feature_store = project
        .get_feature_store()
        .await
        .expect("All projects should have a default feature store");

    println!("{:#?}", feature_store);

    if let Some(feature_group) = feature_store
        .get_feature_group_by_name_and_version(
            std::env::var("HOPSWORKS_FEATURE_GROUP_NAME")
                .unwrap_or_default()
                .as_str(),
            1,
        )
        .await?
    {
        println!("{:#?}", feature_group);

        let mut mini_df = df! [
            "number" => [2i64, 3i64],
            "word" => ["charlie", "dylan"]
        ]?;
        feature_group.insert(&mut mini_df).await?;
    }

    Ok(())
}
