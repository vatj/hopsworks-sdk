use color_eyre::Result;
use reqwest::{Method, StatusCode};

use super::{
    payloads::NewProjectPayload,
    {ProjectAndUserDTO, SingleProjectDTO},
};
use crate::get_hopsworks_client;

pub async fn get_project_and_user_list() -> Result<Vec<ProjectAndUserDTO>> {
    let response = get_hopsworks_client()
        .await
        .request(Method::GET, "project", true, false)
        .await?
        .send()
        .await?;

    if response.status().is_success() {
        Ok(response.json::<Vec<ProjectAndUserDTO>>().await?)
    } else {
        Err(color_eyre::eyre::eyre!(
            "get_project_and_user_list failed with status : {:?}, here is the response :\n{:?}",
            response.status(),
            response.text_with_charset("utf-8").await?
        ))
    }
}

pub async fn create_project(
    new_project_payload: &NewProjectPayload,
) -> Result<Vec<ProjectAndUserDTO>> {
    let response = get_hopsworks_client()
        .await
        .request(Method::POST, "project", true, false)
        .await?
        .json(new_project_payload)
        .send()
        .await?;

    if response.status().is_success() {
        Ok(response.json::<Vec<ProjectAndUserDTO>>().await?)
    } else {
        Err(color_eyre::eyre::eyre!(
            "create_project failed with status : {:?}, here is the response :\n{:?}",
            response.status(),
            response.text_with_charset("utf-8").await?
        ))
    }
}
