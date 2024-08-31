use color_eyre::Result;

use crate::cluster_api::feature_store::storage_connector::service;
use crate::feature_store::storage_connector::{
    FeatureStoreKafkaConnector, FeatureStoreOnlineConnector,
};

pub async fn get_feature_store_kafka_connector(
    feature_store_id: i32,
    external: bool,
) -> Result<FeatureStoreKafkaConnector> {
    Ok(FeatureStoreKafkaConnector::from(
        service::get_feature_store_kafka_connector(feature_store_id, external).await?,
    ))
}

pub async fn get_feature_store_online_connector(
    feature_store_id: i32,
) -> Result<FeatureStoreOnlineConnector> {
    Ok(FeatureStoreOnlineConnector::from(
        service::get_feature_store_online_connector(feature_store_id).await?,
    ))
}
