use color_eyre::Result;

use hopsworks_rs::hopsworks_login;
use polars::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // Load csv files into dataframes
    let mut trans_df = CsvReader::from_path("./example_data/transactions.csv")?
        .with_parse_dates(true)
        .finish()?;

    let _credit_cards_df = CsvReader::from_path("./example_data/credit_cards.csv")?.finish()?;

    let profiles_df = CsvReader::from_path("./example_data/profiles.csv")?
        .with_parse_dates(true)
        .finish()?;

    let age_df = trans_df.left_join(&profiles_df, ["cc_num"], ["cc_num"])?;

    trans_df
        .with_column(
            (&age_df["birthdate"] - &age_df["datetime"])
                .cast(&DataType::Datetime(TimeUnit::Nanoseconds, None))?,
        )?
        .rename("birthdate", "age_at_transaction")?;

    trans_df.with_column(
        trans_df["datetime"].cast(&DataType::Datetime(TimeUnit::Nanoseconds, None))?,
    )?;

    let window_len = "4h";
    let groupby_rolling_options = RollingGroupOptions {
        index_column: String::from("datetime"),
        period: Duration::parse(window_len),
        offset: Duration::parse("0s"),
        closed_window: ClosedWindow::Left,
    };

    trans_df.sort_in_place(["datetime"], vec![false])?;

    let window_agg_df = trans_df
        .select(["datetime", "amount", "cc_num"])?
        .lazy()
        .groupby_rolling([col("cc_num")], groupby_rolling_options)
        .agg([
            col("amount").mean().alias("trans_volume_mavg"),
            col("amount").std(1).alias("trans_volume_mstd"),
            col("amount")
                .count()
                .alias("trans_freq")
                .cast(DataType::Int64),
        ])
        .collect()?;

    let project = hopsworks_login()
        .await
        .expect("Error connecting to Hopsworks:\n");

    let fs = project.get_feature_store().await?;

    let trans_fg = fs
        .get_or_create_feature_group(
            "transactions_fg_3",
            1,
            Some("Transactions data"),
            vec!["cc_num"],
            "datetime",
        )
        .await?;

    trans_fg.insert(&mut trans_df.head(Some(50000))).await?;

    let window_aggs_fg = fs
        .get_or_create_feature_group(
            format!("transactions_{}_aggs_fraud_batch_fg", window_len).as_str(),
            1,
            Some("Aggregate transaction data over {window_len} windows."),
            vec!["cc_num"],
            "datetime",
        )
        .await?;

    window_aggs_fg
        .insert(&mut window_agg_df.head(Some(20)))
        .await?;

    Ok(())
}
