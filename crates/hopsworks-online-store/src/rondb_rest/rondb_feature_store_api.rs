use color_eyre::Result;
use reqwest::Method;

use crate::rondb_rest::get_rondb_rest_client;
use super::{entities::{BatchFeatureVectors, SingleFeatureVector}, payload::{BatchEntriesPayload, SingleEntryPayload}};

pub async fn ping_rondb_rest_server() -> Result<()> {
    get_rondb_rest_client().await.request(Method::GET, "ping").await?.send().await?;
    Ok(())
}

pub async fn get_single_feature_vector(payload: SingleEntryPayload) -> Result<SingleFeatureVector> {
    let resp = get_rondb_rest_client().await.request(Method::POST, "feature_store").await?.send().await?;

    match resp.status {
        Ok(reqwest::StatusCode::ACCEPTED) => Ok(resp.json::<SingleFeatureVector>().await?),
        _ => todo!()
    }
}

pub async fn get_batch_feature_vectors(payload: BatchEntriesPayload) -> Result<BatchFeatureVectors> {
    let resp = get_rondb_rest_client().await.request(Method::POST, "batch_feature_vector").json().await?.send().await?;

    match resp.status() {
        Ok(reqwest::StatusCode::ACCEPTED) => Ok(resp.json::<BatchFeatureVectors>().await?),
        _ => todo!()
    }
}