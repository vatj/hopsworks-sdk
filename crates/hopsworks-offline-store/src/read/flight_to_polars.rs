use color_eyre::Result;
use crate::arrow_flight::client::HopsworksArrowFlightClientBuilder;

use polars::prelude::DataFrame;

pub struct ArrowFlightReadOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) tls: bool,
    pub(crate) root_cert_path: Option<String>,
    pub(crate) token: Option<String>,
}

pub async fn read_with_arrow_flight_client(
    query_str: String,
    _offline_read_options: ArrowFlightReadOptions,
    ondemand_fg_aliases: Vec<String>,
) -> Result<DataFrame> {
    // Create Feature Store Query based on query object obtained via fg.select()
    // let feature_store_query_dto = construct_query(&query_object).await?;

    // Create Arrow Flight Client
    let mut arrow_flight_client = HopsworksArrowFlightClientBuilder::default().build().await?;

    // Extract relevant query string
    // let query_str = feature_store_query_dto
    //     .pit_query_asof
    //     .clone()
    //     .or(Some(feature_store_query_dto.query.clone()))
    //     .unwrap_or_else(|| {
    //         panic!(
    //             "No query string found in Feature Store Query DTO {:#?}.",
    //             feature_store_query_dto
    //         )
    //     });

    // Extract on-demand feature group aliases
    // let on_demand_fg_aliases = feature_store_query_dto
    //     .on_demand_feature_groups
    //     .iter()
    //     .map(|fg| fg.name.clone())
    //     .collect();

    // Use arrow flight client methods to convert query to arrow flight payload
    let query_payload = arrow_flight_client.create_query_object(
        query_object.clone(),
        query_str,
        on_demand_fg_aliases,
    )?;

    let df = arrow_flight_client.read_query(query_payload).await?;

    Ok(df)
}
