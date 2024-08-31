use arrow::record_batch::RecordBatch;
use color_eyre::Result;
use futures::StreamExt;
use polars::prelude::*;
use polars_core::utils::accumulate_dataframes_vertical;

use super::flight_query_builder::build_flight_query;
use crate::{
    arrow_flight::client::HopsworksArrowFlightClientBuilder,
    read::read_options::ArrowFlightReadOptions,
};
use hopsworks_core::feature_store::query::Query;

pub async fn read_with_arrow_flight_client(
    query_object: Query,
    _offline_read_options: Option<ArrowFlightReadOptions>,
    _ondemand_fg_aliases: Vec<String>,
) -> Result<DataFrame> {
    // Create Arrow Flight Client
    let mut arrow_flight_client = HopsworksArrowFlightClientBuilder::default().build().await?;

    let query_payload =
        build_flight_query(query_object, _offline_read_options, _ondemand_fg_aliases).await?;

    let mut record_data_stream = arrow_flight_client.read_query(query_payload).await?;

    let mut dfs: Vec<DataFrame> = vec![];
    while let Some(Ok(record_batch)) = record_data_stream.next().await {
        dfs.push(record_batch_to_dataframe(&record_batch)?);
    }

    Ok(accumulate_dataframes_vertical(dfs)?)
}

fn record_batch_to_dataframe(batch: &RecordBatch) -> Result<DataFrame, PolarsError> {
    let schema = batch.schema();
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (i, column) in batch.columns().iter().enumerate() {
        let arrow = Box::<(dyn polars_arrow::array::Array + 'static)>::from(&**column);
        columns.push(Series::from_arrow(
            schema.fields().get(i).unwrap().name(),
            arrow,
        )?);
    }
    Ok(DataFrame::from_iter(columns))
}
