use crate::cluster_api::feature_store::storage_connector::{
    entities::{FeatureStoreJdbcConnectorDTO, FeatureStoreKafkaConnectorDTO},
    service,
};
use color_eyre::Result;

pub async fn get_feature_store_kafka_connector(
    feature_store_id: i32,
    external: bool,
) -> Result<FeatureStoreKafkaConnectorDTO> {
    service::get_feature_store_kafka_connector(feature_store_id, external).await
}

pub async fn get_feature_store_online_connector(
    feature_store_id: i32,
) -> Result<FeatureStoreJdbcConnectorDTO> {
    service::get_feature_store_online_connector(feature_store_id).await
}
