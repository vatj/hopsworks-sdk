use color_eyre::Result;

use crate::get_hopsworks_client;

use super::entities::FeatureStoreDTO;

pub async fn get_feature_store_by_project_id_and_name(
    project_id: i32,
    feature_store_name: &str,
) -> Result<FeatureStoreDTO> {
    let feature_store_dto = get_hopsworks_client()
        .await
        .send_get(format!("project/{project_id}/featurestores/{feature_store_name}").as_str())
        .await?
        .json()
        .await?;

    Ok(feature_store_dto)
}
