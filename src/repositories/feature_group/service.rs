use color_eyre::Result;
use log::info;
use reqwest::StatusCode;

use super::{entities::FeatureGroupDTO, payloads::NewFeatureGroupPayload};
use crate::get_hopsworks_client;

pub async fn get_feature_group_by_name_and_version(
    feature_store_id: i32,
    name: &str,
    version: i32,
) -> Result<Option<FeatureGroupDTO>> {
    let query_params = [("version", version.to_string())];

    let response = get_hopsworks_client()
        .await
        .send_get_with_query_params(
            format!("featurestores/{feature_store_id}/featuregroups/{name}").as_str(),
            &query_params,
            true,
        )
        .await?;

    match response.status() {
        StatusCode::NOT_FOUND => Ok(None),
        StatusCode::OK => Ok(response.json::<Vec<FeatureGroupDTO>>().await?.pop()),
        _ => panic!(
            "Proper error handling when fetching FG is not implemented yet. Response status: {:?}",
            response.status()
        ), // This is bad...
    }
}

pub async fn get_feature_group_by_id(
    feature_store_id: i32,
    feature_group_id: i32,
) -> Result<Option<FeatureGroupDTO>> {
    let response = get_hopsworks_client()
        .await
        .get_with_project_id_and_auth(
            format!("featurestores/{feature_store_id}/featuregroups/{feature_group_id}").as_str(),
            true,
            true,
        )
        .await?
        .send()
        .await?;

    match response.status() {
        StatusCode::NOT_FOUND => Ok(None),
        StatusCode::OK => Ok(response.json::<Vec<FeatureGroupDTO>>().await?.pop()),
        _ => panic!(
            "Proper error handling when fetching FG is not implemented yet. Response status: {:?}",
            response.status()
        ), // This is bad...
    }
}

pub async fn create_feature_group(
    feature_store_id: i32,
    new_feature_group_payload: &NewFeatureGroupPayload<'_>,
) -> Result<FeatureGroupDTO> {
    let response = get_hopsworks_client()
        .await
        .send_post_json(
            format!("featurestores/{feature_store_id}/featuregroups/").as_str(),
            new_feature_group_payload,
            true,
        )
        .await?;

    match response.status() {
        StatusCode::CREATED => Ok(response.json::<FeatureGroupDTO>().await?),
        _ => {
            info!("{:?}", response.text_with_charset("utf8").await?);
            panic!("Proper error handling when creating FG is not implemented yet.")
        } // This is also bad...
    }
}
