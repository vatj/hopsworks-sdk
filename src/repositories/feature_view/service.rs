use color_eyre::Result;

use super::entities::{FeatureViewDTO, FeatureViewResponseDTO};
use crate::get_hopsworks_client;

pub async fn get_feature_view_by_name_and_version(
    feature_store_id: i32,
    name: &str,
    version: Option<i32>,
) -> Result<Option<FeatureViewDTO>> {
    let mut query_params: Vec<(&str, String)> = vec![];

    if let Some(provided_version) = version {
        query_params.push(("version", provided_version.to_string()));
    }

    let mut feature_view_list = get_hopsworks_client()
        .await
        .send_get_with_query_params(
            format!("featurestores/{feature_store_id}/featureview/{name}").as_str(),
            &query_params,
            true,
        )
        .await?
        .json::<FeatureViewResponseDTO>()
        .await?;

    match feature_view_list.items.pop() {
        Some(feature_view) => Ok(Some(feature_view)),
        None => Ok(None),
    }
}
