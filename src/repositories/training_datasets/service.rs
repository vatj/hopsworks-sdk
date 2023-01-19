use color_eyre::Result;
use reqwest::StatusCode;

use crate::get_hopsworks_client;

use super::entities::TrainingDatasetDTO;

pub async fn get_training_dataset_by_name_and_version(
    feature_store_id: i32,
    name: &str,
    version: Option<i32>,
) -> Result<Option<TrainingDatasetDTO>> {
    let relative_url = format!("featurestores/{feature_store_id}/trainingdatasets/{name}");
    let mut query_params = vec![];
    if let Some(ver) = version {
        query_params.push(("version", ver));
    }

    let res = get_hopsworks_client()
        .await
        .get_with_project_id_and_auth(relative_url.as_str(), true, true)
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
