use color_eyre::Result;
use polars::{lazy::dsl::col, prelude::*};

use hopsworks_rs::{hopsworks_login, HopsworksClientBuilder};

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    env_logger::init();

    // Load csv files into dataframes
    let mut trans_df = CsvReader::from_path("./examples/data/transactions.csv")?
        .with_try_parse_dates(true)
        .finish()?;

    let _credit_cards_df = CsvReader::from_path("./examples/data/credit_cards.csv")?.finish()?;

    let profiles_df = CsvReader::from_path("./examples/data/profiles.csv")?
        .with_try_parse_dates(true)
        .finish()?;

    let age_df = trans_df.left_join(&profiles_df, ["cc_num"], ["cc_num"])?;

    trans_df
        .with_column(&age_df["birthdate"] - &age_df["datetime"])?
        .rename("birthdate", "age_at_transaction")?;

    trans_df.rename("datetime", "datetime")?;

    // Set up rolling window aggregations. If you change window length default value here
    // you must also change it in fraud_batch_training_pipeline.
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
            "transactions_fraud_batch_fg_rust",
            Some(1),
            Some("Transactions data"),
            vec!["cc_num"],
            Some("datetime"),
            true,
        )
        .await?;

    let n_rows = 10;
    trans_fg.insert(&mut trans_df.head(Some(n_rows))).await?;

    let window_aggs_fg = fs
        .get_or_create_feature_group(
            format!("transactions_{}_aggs_fraud_batch_fg_rust", window_len).as_str(),
            Some(1),
            Some(format!("Aggregate transaction data over {} windows.", window_len).as_str()),
            vec!["cc_num"],
            Some("datetime"),
            false,
        )
        .await?;

    window_aggs_fg
        .insert(&mut window_agg_df.head(Some(n_rows)))
        .await?;

    Ok(())
}
