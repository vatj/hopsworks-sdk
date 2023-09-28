use color_eyre::Result;
use reqwest::{StatusCode, Method};

use crate::get_hopsworks_client;

use super::{entities::TrainingDatasetDTO, payloads::NewTrainingDatasetPayload};

pub async fn get_training_dataset_by_name_and_version(
    feature_store_id: i32,
    name: &str,
    version: Option<i32>,
) -> Result<Option<TrainingDatasetDTO>> {
    let mut query_params = vec![];
    if let Some(ver) = version {
        query_params.push(("version", ver));
    }

    let res = get_hopsworks_client()
        .await
        .request(
            Method::GET, 
            format!("featurestores/{feature_store_id}/trainingdatasets/{name}").as_str(), 
            true, 
            true
        )
        .await?
        .query(&query_params)
        .send()
        .await?;

    match res.status() {
        StatusCode::OK => Ok(res.json::<Vec<TrainingDatasetDTO>>().await?.first().cloned()),
        _ => panic!(
            "get_training_dataset_by_name_and_version failed with status : {:?}, here is the response :\n{:?}",
            res.status(),
            res.text_with_charset("utf-8").await?
        ),
    }
}

pub async fn create_training_dataset(
    new_training_dataset_payload: NewTrainingDatasetPayload,
) -> Result<TrainingDatasetDTO> {
    let res = get_hopsworks_client()
        .await
        .request(
            Method::POST,
            format!(
                "featurestores/{}/trainingdatasets",
                new_training_dataset_payload.featurestore_id
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
        _ => panic!(
            "create_training_dataset failed with status : {:?}, here is the response :\n{:?}",
            res.status(),
            res.text_with_charset("utf-8").await?
        ),
    }
}
