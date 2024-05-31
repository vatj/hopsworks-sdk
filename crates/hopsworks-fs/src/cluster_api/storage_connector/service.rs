use color_eyre::Result;
use log::debug;
use reqwest::Method;

use hopsworks_base::get_hopsworks_client;

use super::entities::{
    FeatureStoreJdbcConnectorDTO, FeatureStoreKafkaConnectorDTO, StorageConnectorDTO,
};

pub async fn get_feature_store_kafka_connector(
    feature_store_id: i32,
    external: bool,
) -> Result<FeatureStoreKafkaConnectorDTO> {
    debug!(
        "Fetching kafka storage connector for feature store {}",
        feature_store_id
    );
    let resp = get_hopsworks_client()
        .await
        .request(
            Method::GET,
            format!("featurestores/{feature_store_id}/storageconnectors/kafka_connector/byok")
                .as_str(),
            true,
            true,
        )
        .await?
        .query(&[("external", external)])
        .send()
        .await?;

    match resp.status() {
        reqwest::StatusCode::OK => Ok(resp.json::<FeatureStoreKafkaConnectorDTO>().await?),
        _ => Err(color_eyre::eyre::eyre!(
            "get_feature_store_kafka_connector failed with status : {:?}, here is the response :\n{:?}",
            resp.status(),
            resp.text_with_charset("utf-8").await?
        )),
    }
}

pub async fn get_feature_store_online_connector(
    feature_store_id: i32,
) -> Result<FeatureStoreJdbcConnectorDTO> {
    debug!(
        "Fetching online storage connector for feature store {}",
        feature_store_id
    );
    let resp = get_hopsworks_client()
        .await
        .request(
            Method::GET,
            format!("featurestores/{feature_store_id}/storageconnectors/onlinefeaturestore")
                .as_str(),
            true,
            true,
        )
        .await?
        .send()
        .await?;

    match resp.status() {
        reqwest::StatusCode::OK => Ok(resp.json::<FeatureStoreJdbcConnectorDTO>().await?),
        _ => Err(color_eyre::eyre::eyre!(
            "get_feature_store_online_connector failed with status : {:?}, here is the response :\n{:?}",
            resp.status(),
            resp.text_with_charset("utf-8").await?
        )),
    }
}

pub async fn get_list_feature_store_storage_connectors(
    feature_store_id: i32,
) -> Result<StorageConnectorDTO> {
    debug!(
        "Fetching list storage connectors for feature store {}",
        feature_store_id
    );
    let resp = get_hopsworks_client()
        .await
        .request(
            Method::GET,
            format!("featurestores/{feature_store_id}/storageconnectors").as_str(),
            true,
            true,
        )
        .await?
        .send()
        .await?;

    match resp.status() {
        reqwest::StatusCode::OK => Ok(resp.json::<StorageConnectorDTO>().await?),
        _ => Err(color_eyre::eyre::eyre!(
            "get_list_feature_store_storage_connectors failed with status : {:?}, here is the response :\n{:?}",
            resp.status(),
            resp.text_with_charset("utf-8").await?
        )),
    }
}
