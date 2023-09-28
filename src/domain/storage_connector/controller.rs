use color_eyre::Result;
use crate::repositories::storage_connector::{entities::FeatureStoreKafkaConnectorDTO, service};

pub async fn get_feature_store_kafka_connector(feature_store_id: i32, external: bool) -> Result<FeatureStoreKafkaConnectorDTO> {
    service::get_feature_store_kafka_connector(feature_store_id, external).await
}