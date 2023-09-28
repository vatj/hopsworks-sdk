use color_eyre::Result;
use reqwest::Method;

use crate::get_hopsworks_client;

use super::entities::FeatureStoreDTO;

pub async fn get_feature_store_by_name(feature_store_name: &str) -> Result<FeatureStoreDTO> {
    let feature_store_dto = get_hopsworks_client()
        .await
        .request(Method::GET, format!("featurestores/{feature_store_name}").as_str(), true, true)
        .await?
        .send()
        .await?
        .json::<FeatureStoreDTO>()
        .await?;

    Ok(feature_store_dto)
}
