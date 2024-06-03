use color_eyre::Result;
use log::debug;

use crate::controller::feature_store::storage_connector;

use crate::cluster_api::platform::variables::service::get_loadbalancer_external_domain;
use crate::cluster_api::feature_store::{
        query,
        query::{FeatureStoreQueryDTO, payloads::NewQueryPayload},
    };
use crate::feature_store::query::Query;

pub async fn construct_query(query: &Query) -> Result<FeatureStoreQueryDTO> {
    let query_payload = NewQueryPayload::from(query);

    query::service::construct_query(query_payload).await
}

pub async fn build_mysql_connection_url_from_storage_connector(
    feature_store_id: i32,
) -> Result<String> {
    let online_storage_connector =
        storage_connector::get_feature_store_online_connector(feature_store_id).await?;
    let password = online_storage_connector
        .arguments()
        .iter()
        .find(|arg| arg.get("name") == Some(&String::from("password")))
        .expect("No password key found in online feature store connector arguments")
        .get("value")
        .expect("No password value found in online feature store connector arguments")
        .clone();
    let username = online_storage_connector
        .arguments()
        .iter()
        .find(|arg| arg.get("name") == Some(&String::from("user")))
        .expect("No user key found in online feature store connector arguments")
        .get("value")
        .expect("No user value found in online feature store connector arguments");
    let mut connection_string = online_storage_connector
        .connection_string()
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