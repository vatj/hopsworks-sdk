use color_eyre::Result;
use connectorx::prelude::{Arrow2Destination, Dispatcher, MySQLSource};
use connectorx::sources::mysql::BinaryProtocol;
use connectorx::sql::CXQuery;
use connectorx::transports::MySQLArrow2Transport;
use log::debug;
use polars::prelude::DataFrame;

use crate::clients::arrow_flight::client::HopsworksArrowFlightClientBuilder;
use crate::core::feature_store::storage_connector;
use crate::feature_store::query::read_option::{OfflineReadOptions, OnlineReadOptions};
use crate::repositories::platform::variables::service::get_loadbalancer_external_domain;
use crate::{
    feature_store::query::Query,
    repositories::feature_store::{
        query,
        query::{entities::FeatureStoreQueryDTO, payloads::NewQueryPayload},
    },
};

pub async fn construct_query(query: Query) -> Result<FeatureStoreQueryDTO> {
    let query_payload = NewQueryPayload::from(query);

    query::service::construct_query(query_payload).await
}

pub async fn build_mysql_connection_url_from_storage_connector(
    feature_store_id: i32,
) -> Result<String> {
    let online_storage_connector =
        storage_connector::get_feature_store_online_connector(feature_store_id).await?;
    let password = online_storage_connector
        .arguments
        .iter()
        .find(|arg| arg.get("name") == Some(&String::from("password")))
        .expect("No password key found in online feature store connector arguments")
        .get("value")
        .expect("No password value found in online feature store connector arguments")
        .clone();
    let username = online_storage_connector
        .arguments
        .iter()
        .find(|arg| arg.get("name") == Some(&String::from("user")))
        .expect("No user key found in online feature store connector arguments")
        .get("value")
        .expect("No user value found in online feature store connector arguments");
    let mut connection_string = online_storage_connector
        .connection_string
        .replace("jdbc:", "");
    let end_range = connection_string[8..].find(':').unwrap();
    let mut host = get_loadbalancer_external_domain().await?;
    if host.is_empty() {
        host = std::env::var("HOPSWORKS_EXTERNAL_HOST").unwrap();
    }
    connection_string.replace_range(8..(end_range + 8), &host);
    connection_string = connection_string
        .replace(
            "mysql://",
            format!("mysql://{}:{}@", username, password).as_str(),
        )
        .replace("?useSSL=false&allowPublicKeyRetrieval=true", "");
    debug!("Connection string: {}", connection_string);

    Ok(connection_string)
}

pub async fn read_query_from_online_feature_store(
    query: &Query,
    online_read_options: Option<OnlineReadOptions>,
) -> Result<DataFrame> {
    let _online_read_options = online_read_options.unwrap_or_default();
    let connection_string = build_mysql_connection_url_from_storage_connector(
        query.left_feature_group().feature_store_id(),
    )
    .await?;
    let builder = MySQLSource::<BinaryProtocol>::new(connection_string.as_str(), 2).unwrap();

    let constructed_query = construct_query(query.clone()).await?;
    let queries = vec![CXQuery::from(&constructed_query.query_online)];
    let mut destination = Arrow2Destination::new();

    let dispatcher = Dispatcher::<
        MySQLSource<BinaryProtocol>,
        Arrow2Destination,
        MySQLArrow2Transport<BinaryProtocol>,
    >::new(builder, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    let df: DataFrame = destination.polars().unwrap();

    Ok(df)
}

pub async fn read_with_arrow_flight_client(
    query_object: Query,
    offline_read_options: Option<OfflineReadOptions>,
) -> Result<DataFrame> {
    // Create Feature Store Query based on query object obtained via fg.select()
    let _offline_read_options = offline_read_options.unwrap_or_default();
    let feature_store_query_dto = construct_query(query_object.clone()).await?;

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
    let query_payload = arrow_flight_client.create_query_object(
        query_object.clone(),
        query_str,
        on_demand_fg_aliases,
    )?;

    let df = arrow_flight_client.read_query(query_payload).await?;

    Ok(df)
}
