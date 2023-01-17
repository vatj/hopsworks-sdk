use color_eyre::Result;
use reqwest::StatusCode;

use crate::get_hopsworks_client;

use super::{entities::FeatureStoreQueryDTO, payloads::NewQueryPayload};

pub async fn construct_query<'a>(
    query_payload: NewQueryPayload<'_>,
) -> Result<FeatureStoreQueryDTO> {
    let res = get_hopsworks_client()
        .await
        .put_with_project_id_and_auth("featurestores/query", true, true)
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
