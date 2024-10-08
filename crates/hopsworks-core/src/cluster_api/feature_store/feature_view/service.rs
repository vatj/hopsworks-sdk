use color_eyre::Result;
use reqwest::{Method, StatusCode};

use super::{
    payloads::{FeatureViewBatchQueryPayload, NewFeatureViewPayload},
    FeatureViewDTO,
};
use crate::cluster_api::{
    feature_store::{
        query::QueryDTO,
        training_dataset::{
            payloads::{NewTrainingDatasetPayload, TrainingDatasetComputeJobConfigPayload},
            TrainingDatasetDTO,
        },
    },
    platform::job::JobDTO,
};
use crate::get_hopsworks_client;

pub async fn get_feature_view_by_name_and_version(
    feature_store_id: i32,
    name: &str,
    version: Option<i32>,
) -> Result<Option<FeatureViewDTO>> {
    let query_params = [("expand", "features"), ("expand", "query")];
    let base_relative_url = format!("featurestores/{feature_store_id}/featureview/{name}");
    let relative_url = match version {
        Some(ver) => format!("{base_relative_url}/version/{ver}"),
        None => base_relative_url,
    };

    let res = get_hopsworks_client()
        .await
        .request(Method::GET, relative_url.as_str(), true, true)
        .await?
        .query(&query_params)
        .send()
        .await?;

    match res.status() {
        StatusCode::OK => Ok(Some(res.json::<FeatureViewDTO>().await?)),
        _ => Err(color_eyre::eyre::eyre!(
            "get_feature_view failed with status : {:?}, here is the response :\n{:?}",
            res.status(),
            res.text_with_charset("utf-8").await?
        )),
    }
}

pub async fn create_feature_view(
    feature_store_id: i32,
    new_feature_view_payload: NewFeatureViewPayload,
) -> Result<FeatureViewDTO> {
    let res = get_hopsworks_client()
        .await
        .request(
            Method::POST,
            format!("featurestores/{}/featureview", feature_store_id).as_str(),
            true,
            true,
        )
        .await?
        .json(&new_feature_view_payload)
        .send()
        .await?;

    match res.status() {
        StatusCode::CREATED => Ok(res.json::<FeatureViewDTO>().await?),
        _ => Err(color_eyre::eyre::eyre!(
            "create_feature_view failed with status : {:?}, here is the response :\n{:?}",
            res.status(),
            res.text_with_charset("utf-8").await?
        )),
    }
}

// This is left here to emulate the feature-store-api
pub async fn create_training_dataset_attached_to_feature_view(
    name: &str,
    version: i32,
    new_training_dataset_payload: NewTrainingDatasetPayload,
) -> Result<TrainingDatasetDTO> {
    let res = get_hopsworks_client()
        .await
        .request(
            Method::POST,
            format!(
                "featurestores/{}/featureview/{name}/version/{version}/trainingdatasets",
                new_training_dataset_payload.featurestore_id,
            )
            .as_str(),
            true,
            true,
        )
        .await?
        .json(&new_training_dataset_payload)
        .send()
        .await?;

    match res.status() {
        StatusCode::CREATED => Ok(res.json::<TrainingDatasetDTO>().await?),
        _ => Err(color_eyre::eyre::eyre!(
            "create_training_dataset failed with status : {:?}, here is the response :\n{:?}",
            res.status(),
            res.text_with_charset("utf-8").await?
        )),
    }
}

pub async fn compute_training_dataset_attached_to_feature_view(
    feature_store_id: i32,
    name: &str,
    version: i32,
    dataset_version: i32,
    job_config: TrainingDatasetComputeJobConfigPayload,
) -> Result<JobDTO> {
    let res = get_hopsworks_client()
        .await
        .request(
            Method::POST,
            format!(
                "featurestores/{feature_store_id}/featureview/{name}/version/{version}/trainingdatasets/version/{dataset_version}/compute",
            )
            .as_str(),
            true,
            true,
        )
        .await?
        .json(&job_config)
        .send()
        .await?;

    match res.status() {
        StatusCode::CREATED => Ok(res.json::<JobDTO>().await?),
        _ => Err(color_eyre::eyre::eyre!(
            "create_training_dataset failed with status : {:?}, here is the response :\n{:?}",
            res.status(),
            res.text_with_charset("utf-8").await?
        )),
    }
}

pub async fn get_feature_view_batch_query(
    feature_store_id: i32,
    name: &str,
    version: i32,
    batch_query_payload: FeatureViewBatchQueryPayload,
) -> Result<QueryDTO> {
    let resp = get_hopsworks_client()
        .await
        .request(
            Method::GET,
            format!(
                "featurestores/{feature_store_id}/featureview/{name}/version/{version}/query/batch",
            )
            .as_str(),
            true,
            true,
        )
        .await?
        .json(&batch_query_payload)
        .send()
        .await?;

    match resp.status() {
        StatusCode::OK => Ok(resp.json::<QueryDTO>().await?),
        _ => Err(color_eyre::eyre::eyre!(
            "get_feature_view_batch_query failed with status : {:?}, here is the response :\n{:?}",
            resp.status(),
            resp.text_with_charset("utf-8").await?
        )),
    }
}

pub async fn delete_feature_view_by_name_and_version(
    feature_store_id: i32,
    feature_view_name: &str,
    feature_view_version: i32,
) -> Result<()> {
    let response = get_hopsworks_client()
        .await
        .request(
            Method::DELETE,
            format!("featurestores/{feature_store_id}/featureview/{feature_view_name}/version/{feature_view_version}").as_str(),
            true,
            true,
        )
        .await?
        .send()
        .await?;

    match response.status() {
        StatusCode::OK => Ok(()),
        _ => Err(color_eyre::eyre::eyre!(
            "delete_feature_view_by_name_and_version failed with status : {:?}, here is the response :\n{:?}",
            response.status(),
            response.text_with_charset("utf-8").await?
        )),
    }
}

pub async fn delete_feature_view_by_name(
    feature_store_id: i32,
    feature_view_name: &str,
) -> Result<()> {
    let response = get_hopsworks_client()
        .await
        .request(
            Method::DELETE,
            format!("featurestores/{feature_store_id}/featureview/{feature_view_name}").as_str(),
            true,
            true,
        )
        .await?
        .send()
        .await?;

    match response.status() {
        StatusCode::OK => Ok(()),
        _ => Err(color_eyre::eyre::eyre!(
            "delete_feature_view_by_name failed with status : {:?}, here is the response :\n{:?}",
            response.status(),
            response.text_with_charset("utf-8").await?
        )),
    }
}
