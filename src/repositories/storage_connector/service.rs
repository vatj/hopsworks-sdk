use color_eyre::Result;
use log::debug;
use reqwest::Method;

use crate::get_hopsworks_client;

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
    Ok(get_hopsworks_client()
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
        .await?
        .json::<FeatureStoreKafkaConnectorDTO>()
        .await?)
}

pub async fn get_feature_store_online_connector(
    feature_store_id: i32,
) -> Result<FeatureStoreJdbcConnectorDTO> {
    debug!(
        "Fetching online storage connector for feature store {}",
        feature_store_id
    );
    Ok(get_hopsworks_client()
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
        .await?
        .json::<FeatureStoreJdbcConnectorDTO>()
        .await?)
}

pub async fn get_list_feature_store_storage_connectors(
    feature_store_id: i32,
) -> Result<StorageConnectorDTO> {
    debug!(
        "Fetching list storage connectors for feature store {}",
        feature_store_id
    );
    Ok(get_hopsworks_client()
        .await
        .request(
            Method::GET,
            format!("featurestores/{feature_store_id}/storageconnectors").as_str(),
            true,
            true,
        )
        .await?
        .send()
        .await?
        .json::<StorageConnectorDTO>()
        .await?)
}
