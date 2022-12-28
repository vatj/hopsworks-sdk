use color_eyre::Result;

use hopsworks_rs::hopsworks_login;
use polars::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // Load csv files into dataframes
    let mut trans_df = CsvReader::from_path("./example_data/transactions.csv")
        .unwrap()
        .with_parse_dates(true)
        .finish()
        .unwrap();

    let _credit_cards_df = CsvReader::from_path("./example_data/credit_cards.csv")
        .unwrap()
        .finish()
        .unwrap();

    let profiles_df = CsvReader::from_path("./example_data/profiles.csv")
        .unwrap()
        .with_parse_dates(true)
        .finish()
        .unwrap();

    let age_df = trans_df
        .left_join(&profiles_df, ["cc_num"], ["cc_num"])
        .unwrap();

    trans_df
        .with_column(&age_df["birthdate"] - &age_df["datetime"])
        .unwrap()
        .rename("birthdate", "age_at_transaction")
        .unwrap();

    let project = hopsworks_login()
        .await
        .expect("Error connecting to Hopsworks:\n");

    let fs = project.get_feature_store().await?;

    let trans_fg = fs.get_or_create_feature_group(
        "transactions_fg",
        1,
        Some("Transactions data"),
        vec!["cc_num"],
        "datetime",
    );

    Ok(())
}
