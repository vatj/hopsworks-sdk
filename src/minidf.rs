use color_eyre::Result;
use polars::prelude::*;
use std::io::Cursor;

pub async fn get_example_df() -> Result<DataFrame> {
    let data: Vec<u8> = reqwest::Client::new()
        .get("https://j.mp/iriscsv")
        .send()
        .await?
        .text()
        .await?
        .bytes()
        .collect();

    let df = CsvReader::new(Cursor::new(data))
        .has_header(true)
        .finish()?
        .lazy()
        .filter(col("sepal_length").gt(5))
        .group_by([col("species")])
        .agg([col("*").sum()])
        .collect()?;

    Ok(df)
}

pub async fn get_mini_df() -> Result<DataFrame> {
    let df: DataFrame = df! [
        "number" => [2i64, 3i64],
        "word" => ["charlie", "dylan"]
    ]
    .unwrap();

    Ok(df)
}
