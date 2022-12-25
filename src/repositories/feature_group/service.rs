use color_eyre::Result;

use super::{entities::FeatureGroupDTO, payloads::NewFeatureGroupPayload};
use crate::get_hopsworks_client;

pub async fn get_feature_group_by_name_and_version(
    feature_store_id: i32,
    name: &str,
    version: i32,
) -> Result<Option<FeatureGroupDTO>> {
    let query_params = [("version", version.to_string())];

    let mut feature_group_list = get_hopsworks_client()
        .await
        .send_get_with_query_params(
            format!("featurestores/{feature_store_id}/featuregroups/{name}").as_str(),
            &query_params,
            true,
        )
        .await?
        .json::<Vec<FeatureGroupDTO>>()
        .await?;

    match feature_group_list.pop() {
        Some(feature_group) => Ok(Some(feature_group)),
        None => Ok(None),
    }
}

pub async fn create_feature_group(
    feature_store_id: i32,
    new_feature_group_payload: &NewFeatureGroupPayload<'_>,
) -> Result<FeatureGroupDTO> {
    Ok(get_hopsworks_client()
        .await
        .send_post_json(
            format!("featurestores/{feature_store_id}/featuregroups/").as_str(),
            new_feature_group_payload,
            true,
        )
        .await?
        .json::<FeatureGroupDTO>()
        .await?)
}
