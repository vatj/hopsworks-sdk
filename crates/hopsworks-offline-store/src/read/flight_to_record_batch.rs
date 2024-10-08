use arrow::record_batch::RecordBatch;
use color_eyre::Result;
use futures::StreamExt;

use hopsworks_core::feature_store::query::Query;

use super::flight_query_builder;
use crate::arrow_flight::client::HopsworksArrowFlightClientBuilder;
use crate::read::read_options::ArrowFlightReadOptions;

pub async fn read_to_record_batch_with_arrow_flight_client(
    query_object: Query,
    _offline_read_options: Option<ArrowFlightReadOptions>,
    _ondemand_fg_aliases: Vec<String>,
) -> Result<Vec<RecordBatch>> {
    // Convert query to arrow flight payload
    let query_payload = flight_query_builder::build_flight_query(
        query_object,
        _offline_read_options,
        _ondemand_fg_aliases,
    )
    .await?;

    // Create Arrow Flight Client
    let mut arrow_flight_client = HopsworksArrowFlightClientBuilder::default().build().await?;

    // Read query from Arrow Flight
    let mut record_data_stream = arrow_flight_client.read_query(query_payload).await?;

    let mut batches: Vec<RecordBatch> = vec![];
    while let Some(record_batch) = record_data_stream.next().await {
        batches.push(record_batch?);
    }

    Ok(batches)
}
