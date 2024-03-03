use color_eyre::Result;
use reqwest::{Method, StatusCode};

use crate::get_hopsworks_client;

use super::entities::FeatureStoreDTO;

pub async fn get_feature_store_by_name(feature_store_name: &str) -> Result<FeatureStoreDTO> {
    let resp = get_hopsworks_client()
        .await
        .request(
            Method::GET,
            format!("featurestores/{feature_store_name}").as_str(),
            true,
            true,
        )
        .await?
        .send()
        .await?;

    match resp.status() {
        StatusCode::OK => Ok(resp.json::<FeatureStoreDTO>().await?),
        _ => Err(color_eyre::eyre::eyre!(
            "get_feature_store_by_name failed with status : {:?}, here is the response :\n{:?}",
            resp.status(),
            resp.text_with_charset("utf-8").await?
        )),
    }
}
