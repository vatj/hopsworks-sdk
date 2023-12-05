use color_eyre::Result;
use std::collections::HashMap;

use hopsworks_rs::{
    feature_store::feature_view::transformation_function::TransformationFunction, hopsworks_login,
    HopsworksClientBuilder,
};

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    env_logger::init();
    // Set up rolling window aggregations. If you have changed window length default value in
    // fraud_batch_feature_pipeline, you must change it here accordingly.
    let window_len = "4h";

    let project = hopsworks_login(Some(
        HopsworksClientBuilder::default()
            .with_url(std::env::var("HOPSWORKS_URL").unwrap_or_default().as_str()),
    ))
    .await
    .expect("Error connecting to Hopsworks.");

    let fs = project.get_feature_store().await?;

    let trans_fg = fs
        .get_feature_group_by_name_and_version(
            "transactions_fraud_batch_fg_rust",
            1,
        )
        .await?
        .expect("Feature Group not found. Did you run the fraud_batch_ingestion_pipeline example first?");

    let window_aggs_fg = fs
        .get_feature_group_by_name_and_version(
            format!(
                "transactions_{}_aggs_fraud_batch_fg_rust",
                window_len
            )
            .as_str(),
            1,
        )
        .await?
        .expect("Feature Group not found. Check that window_len matches the fraud_batch_ingestion_pipeline example.");

    let query = trans_fg.select(vec!["cc_num", "datetime", "amount"])?.join(
        window_aggs_fg.select(vec![
            "cc_num",
            "datetime",
            "amount_mean",
            "amount_std",
            "amount_min",
            "amount_max",
        ])?,
        None,
    );

    let min_max_scaler = fs
        .get_transformation_function("min_max_scaler", None)
        .await?;
    let _label_encoder = fs
        .get_transformation_function("label_encoder", None)
        .await?;

    let mut transformation_functions = HashMap::<String, TransformationFunction>::new();
    transformation_functions.insert("amount".to_owned(), min_max_scaler.unwrap());

    let feature_view = fs
        .create_feature_view(
            "transactions_and_fraud_view_rust",
            1,
            query,
            Some(transformation_functions),
        )
        .await?;

    let training_df = feature_view.read_with_arrow_flight_client().await?;

    println!("The training dataset: {:#?}", training_df.head(Some(10)));

    feature_view.create_attached_training_dataset().await?;

    // let my_new_dataset = fs
    //     .get_training_dataset_by_name_and_version("trans_view_{iteration}_rust", Some(1))
    //     .await?;

    // println!("The dataset: {:#?}", my_new_dataset);

    Ok(())
}
