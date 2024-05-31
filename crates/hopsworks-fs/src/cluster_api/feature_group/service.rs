use color_eyre::Result;
use reqwest::{Method, StatusCode};

use super::{entities::FeatureGroupDTO, payloads::NewFeatureGroupPayload};
use hopsworks_base::get_hopsworks_client;

pub async fn get_feature_group_by_name_and_version(
    feature_store_id: i32,
    name: &str,
    version: i32,
) -> Result<Option<FeatureGroupDTO>> {
    let query_params = [("version", version.to_string())];

    let response = get_hopsworks_client()
        .await
        .request(
            Method::GET,
            format!("featurestores/{feature_store_id}/featuregroups/{name}").as_str(),
            true,
            true,
        )
        .await?
        .query(&query_params)
        .send()
        .await?;

    match response.status() {
        StatusCode::NOT_FOUND => Ok(None),
        StatusCode::OK => Ok(response.json::<Vec<FeatureGroupDTO>>().await?.pop()),
        _ => Err(color_eyre::eyre::eyre!(
            "get_feature_group_by_name_and_version failed with status : {:?}, here is the response :\n{:?}",
            response.status(),
            response.text_with_charset("utf-8").await?
        )),
    }
}

pub async fn get_latest_feature_group_by_name(
    feature_store_id: i32,
    name: &str,
) -> Result<Option<FeatureGroupDTO>> {
    let response = get_hopsworks_client()
        .await
        .request(
            Method::GET,
            format!("featurestores/{feature_store_id}/featuregroups/{name}").as_str(),
            true,
            true,
        )
        .await?
        .send()
        .await?;

    match response.status() {
        StatusCode::NOT_FOUND => Ok(None),
        StatusCode::OK => Ok(response.json::<Vec<FeatureGroupDTO>>().await?.pop()),
        _ => Err(color_eyre::eyre::eyre!(
            "get_latest_feature_group_by_name failed with status : {:?}, here is the response :\n{:?}",
            response.status(),
            response.text_with_charset("utf-8").await?
        )),
    }
}

pub async fn get_feature_group_by_id(
    feature_store_id: i32,
    feature_group_id: i32,
) -> Result<Option<FeatureGroupDTO>> {
    let resp = get_hopsworks_client()
        .await
        .request(
            Method::GET,
            format!("featurestores/{feature_store_id}/featuregroups/{feature_group_id}").as_str(),
            true,
            true,
        )
        .await?
        .send()
        .await?;

    match resp.status() {
        StatusCode::NOT_FOUND => Ok(None),
        StatusCode::OK => Ok(resp.json::<Vec<FeatureGroupDTO>>().await?.pop()),
        _ => Err(color_eyre::eyre::eyre!(
            "get_feature_group_by_id failed with status : {:?}, here is the response :\n{:?}",
            resp.status(),
            resp.text_with_charset("utf-8").await?
        )),
    }
}

pub async fn create_feature_group(
    feature_store_id: i32,
    new_feature_group_payload: &NewFeatureGroupPayload,
) -> Result<FeatureGroupDTO> {
    let response = get_hopsworks_client()
        .await
        .request(
            Method::POST,
            format!("featurestores/{feature_store_id}/featuregroups/").as_str(),
            true,
            true,
        )
        .await?
        .json(new_feature_group_payload)
        .send()
        .await?;

    match response.status() {
        StatusCode::CREATED => Ok(response.json::<FeatureGroupDTO>().await?),
        _ => Err(color_eyre::eyre::eyre!(
            "create_feature_group failed with status : {:?}, here is the response :\n{:?}",
            response.status(),
            response.text_with_charset("utf-8").await?
        )),
    }
}
