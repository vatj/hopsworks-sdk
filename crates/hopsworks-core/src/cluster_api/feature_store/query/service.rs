use color_eyre::Result;
use reqwest::{Method, StatusCode};

use crate::get_hopsworks_client;

use super::{payloads::NewQueryPayload, FeatureStoreQueryDTO};

pub async fn construct_query(query_payload: NewQueryPayload) -> Result<FeatureStoreQueryDTO> {
    let res = get_hopsworks_client()
        .await
        .request(Method::PUT, "featurestores/query", true, true)
        .await?
        .json(&query_payload)
        .send()
        .await?;

    match res.status() {
        StatusCode::OK => Ok(res.json::<FeatureStoreQueryDTO>().await?),
        _ => panic!(
            "Failed with status {:?}, here is the response : \n{:?}.",
            res.status(),
            res.text_with_charset("utf-8").await
        ),
    }
}

pub async fn get_batch_query_by_feature_view_name_and_version(
    feature_store_id: i32,
    name: &str,
    version: i32,
) -> Result<FeatureStoreQueryDTO> {
    let res = get_hopsworks_client()
        .await
        .request(
            Method::GET,
            format!(
            "featurestores/{feature_store_id}/feature_view/{name}/version/{version}/query/batch"
        )
            .as_str(),
            true,
            true,
        )
        .await?
        .send()
        .await?;

    match res.status() {
        StatusCode::OK => Ok(res.json::<FeatureStoreQueryDTO>().await?),
        _ => panic!(
            "Failed with status {:?}, here is the response : \n{:?}.",
            res.status(),
            res.text_with_charset("utf-8").await
        ),
    }
}
