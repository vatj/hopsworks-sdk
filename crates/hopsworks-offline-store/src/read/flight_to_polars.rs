use color_eyre::Result;
use polars::prelude::DataFrame;

use hopsworks_core::feature_store::query::Query;
use hopsworks_core::controller::feature_store::query::construct_query;

use crate::arrow_flight::client::HopsworksArrowFlightClientBuilder;
use crate::read::read_options::ArrowFlightReadOptions;


pub async fn read_with_arrow_flight_client(
    query_object: Query,
    _offline_read_options: Option<ArrowFlightReadOptions>,
    _ondemand_fg_aliases: Vec<String>,
) -> Result<DataFrame> {
    // Create Feature Store Query based on query object obtained via fg.select()
    let feature_store_query_dto = construct_query(&query_object).await?;

    // Create Arrow Flight Client
    let mut arrow_flight_client = HopsworksArrowFlightClientBuilder::default().build().await?;

    // Extract relevant query string
    let query_str = feature_store_query_dto
        .pit_query_asof
        .clone()
        .or(Some(feature_store_query_dto.query.clone()))
        .unwrap_or_else(|| {
            panic!(
                "No query string found in Feature Store Query DTO {:#?}.",
                feature_store_query_dto
            )
        });

    // Extract on-demand feature group aliases
    let on_demand_fg_aliases = feature_store_query_dto
        .on_demand_feature_groups
        .iter()
        .map(|fg| fg.name.clone())
        .collect();

    // Use arrow flight client methods to convert query to arrow flight payload
    let query_payload = arrow_flight_client.create_flight_query(
        query_object.clone(),
        query_str,
        on_demand_fg_aliases,
    )?;

    let df = arrow_flight_client.read_query(query_payload).await?;

    Ok(df)
}
