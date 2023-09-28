use color_eyre::Result;
use reqwest::Method;

use crate::get_hopsworks_client;

use super::entities::FeatureStoreKafkaConnectorDTO;

pub async fn get_kafka_storage_connector(feature_store_id: i32, external: bool) -> Result<FeatureStoreKafkaConnectorDTO> {
    Ok(get_hopsworks_client()
        .await
        .request(
            Method::GET, 
            format!("featurestores/{feature_store_id}/storageconnectors/kafka_connector/byok").as_str(), 
            true, 
            true
        )
        .await?
        .query(&[("external", external)])
        .send()
        .await?
        .json::<FeatureStoreKafkaConnectorDTO>()
        .await?)
}