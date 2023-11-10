use color_eyre::Result;
use connectorx::prelude::{Arrow2Destination, Dispatcher, MySQLSource};
use connectorx::sources::mysql::BinaryProtocol;
use connectorx::sql::CXQuery;
use connectorx::transports::MySQLArrow2Transport;
use log::debug;
use polars::prelude::DataFrame;

use crate::repositories::storage_connector::entities::FeatureStoreJdbcConnectorDTO;
use crate::repositories::variables::service::get_loadbalancer_external_domain;
use crate::{
    api::query::entities::Query,
    repositories::{
        query,
        query::{entities::FeatureStoreQueryDTO, payloads::NewQueryPayload},
    },
};

pub async fn construct_query(query: Query) -> Result<FeatureStoreQueryDTO> {
    let query_payload = NewQueryPayload::from(query);

    query::service::construct_query(query_payload).await
}

pub async fn read_query_from_online_feature_store(
    query: Query,
    feature_store_jdbc_connector: FeatureStoreJdbcConnectorDTO,
) -> Result<DataFrame> {
    let password = feature_store_jdbc_connector
        .arguments
        .iter()
        .find(|arg| arg.get("name") == Some(&String::from("password")))
        .expect("No password key found in online feature store connector arguments")
        .get("value")
        .expect("No password value found in online feature store connector arguments")
        .clone();
    let username = feature_store_jdbc_connector
        .arguments
        .iter()
        .find(|arg| arg.get("name") == Some(&String::from("user")))
        .expect("No user key found in online feature store connector arguments")
        .get("value")
        .expect("No user value found in online feature store connector arguments");
    let mut connection_string = feature_store_jdbc_connector
        .connection_string
        .replace("jdbc:", "");
    let end_range = connection_string[8..].find(':').unwrap();
    connection_string.replace_range(
        8..(end_range + 8),
        &get_loadbalancer_external_domain().await?,
    );
    connection_string = connection_string
        .replace(
            "mysql://",
            format!("mysql://{}:{}@", username, password).as_str(),
        )
        .replace("?useSSL=false&allowPublicKeyRetrieval=true", "");
    let builder = MySQLSource::<BinaryProtocol>::new(connection_string.as_str(), 2).unwrap();

    let constructed_query = construct_query(query).await?;
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
