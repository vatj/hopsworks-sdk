use color_eyre::Result;
use hopsworks::HopsworksClientBuilder;

use polars::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let project = hopsworks::login(Some(
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

    if let Some(mut feature_group) = feature_store
        .get_feature_group(
            std::env::var("HOPSWORKS_FEATURE_GROUP_NAME")
                .unwrap_or_default()
                .as_str(),
            Some(1),
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
