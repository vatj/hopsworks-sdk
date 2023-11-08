use color_eyre::Result;
use polars::{lazy::dsl::col, prelude::*};
use std::collections::HashMap;

use hopsworks_rs::{
    api::transformation_function::entities::TransformationFunction,
    clients::rest_client::HopsworksClientBuilder,
    domain::{
        query::controller::construct_query,
        training_dataset::controller::create_training_dataset_attached_to_feature_view,
    },
    hopsworks_login,
};

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    env_logger::init();
    let iteration =
        std::env::var("HOPSWORKS_FRAUD_BATCH_ITERATION").unwrap_or_else(|_| "1".to_owned());

    // Load csv files into dataframes
    let mut trans_df = CsvReader::from_path("./example_data/transactions.csv")?
        .with_try_parse_dates(true)
        .finish()?;

    let _credit_cards_df = CsvReader::from_path("./example_data/credit_cards.csv")?.finish()?;

    let profiles_df = CsvReader::from_path("./example_data/profiles.csv")?
        .with_try_parse_dates(true)
        .finish()?;

    let age_df = trans_df.left_join(&profiles_df, ["cc_num"], ["cc_num"])?;

    trans_df
        .with_column(
            (&age_df["birthdate"] - &age_df["datetime"])
                .cast(&DataType::Datetime(TimeUnit::Nanoseconds, None))?,
        )?
        .rename("birthdate", "age_at_transaction")?;

    trans_df.with_column(
        1e-3.mul(&trans_df["datetime"])
            .cast(&DataType::Datetime(TimeUnit::Nanoseconds, None))?,
    )?;

    let window_len = "4h";
    let group_by_rolling_options = RollingGroupOptions {
        index_column: "datetime".into(),
        period: Duration::parse(window_len),
        offset: Duration::parse("0s"),
        closed_window: ClosedWindow::Left,
        check_sorted: true,
    };

    trans_df.sort_in_place(["datetime"], vec![false], true)?;

    let window_agg_df = trans_df
        .select(["datetime", "amount", "cc_num"])?
        .lazy()
        .groupby_rolling(col("cc_num"), [col("datetime")], group_by_rolling_options)
        .agg([
            col("amount").mean().alias("trans_volume_mavg"),
            col("amount").std(1).alias("trans_volume_mstd"),
            col("amount")
                .count()
                .alias("trans_freq")
                .cast(DataType::Int64),
        ])
        .collect()?;

    let project = hopsworks_login(Some(
        HopsworksClientBuilder::default()
            .with_url(std::env::var("HOPSWORKS_URL").unwrap_or_default().as_str()),
    ))
    .await
    .expect("Error connecting to Hopsworks:\n");

    let fs = project.get_feature_store().await?;

    let trans_fg = fs
        .get_or_create_feature_group(
            format!("transactions_fraud_batch_fg_{iteration}_rust").as_str(),
            1,
            Some("Transactions data"),
            vec!["cc_num"],
            "datetime",
            true,
        )
        .await?;

    let n_rows = 5000;
    trans_fg.insert(&mut trans_df.head(Some(n_rows))).await?;

    let window_aggs_fg = fs
        .get_or_create_feature_group(
            format!(
                "transactions_{}_aggs_fraud_batch_fg_{iteration}_rust",
                window_len
            )
            .as_str(),
            1,
            Some(format!("Aggregate transaction data over {} windows.", window_len).as_str()),
            vec!["cc_num"],
            "datetime",
            false,
        )
        .await?;

    window_aggs_fg
        .insert(&mut window_agg_df.head(Some(n_rows)))
        .await?;

    let query = trans_fg.select(vec!["cc_num", "datetime", "amount"])?;

    println!("Query: {:#?}", construct_query(query.clone()).await?);

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
            format!("trans_view_{iteration}_rust").as_str(),
            1,
            query,
            transformation_functions,
        )
        .await?;

    let fetched_view = fs
        .get_feature_view(format!("trans_view_{iteration}_rust").as_str(), Some(1))
        .await?
        .unwrap();

    println!("The fetched feature view: {:#?}", fetched_view);

    create_training_dataset_attached_to_feature_view(feature_view).await?;

    // let my_new_dataset = fs
    //     .get_training_dataset_by_name_and_version("trans_view_{iteration}_rust", Some(1))
    //     .await?;

    // println!("The dataset: {:#?}", my_new_dataset);

    Ok(())
}
