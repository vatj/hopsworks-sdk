use color_eyre::Result;
use reqwest::{Method, StatusCode};

use super::{
    entities::{ProjectAndUserDTO, SingleProjectDTO},
    payloads::NewProjectPayload,
};
use crate::get_hopsworks_client;

pub async fn get_project_and_user_list() -> Result<Vec<ProjectAndUserDTO>> {
    Ok(get_hopsworks_client()
        .await
        .request(Method::GET, "project", true, false)
        .await?
        .send()
        .await?
        .json::<Vec<ProjectAndUserDTO>>()
        .await?)
}

pub async fn create_project(
    new_project_payload: &NewProjectPayload<'_>,
) -> Result<Vec<ProjectAndUserDTO>> {
    Ok(get_hopsworks_client()
        .await
        .request(Method::POST, "project", true, false)
        .await?
        .json(new_project_payload)
        .send()
        .await?
        .json::<Vec<ProjectAndUserDTO>>()
        .await?)
}

pub async fn get_client_project() -> Result<SingleProjectDTO> {
    let resp = get_hopsworks_client()
        .await
        .request(Method::GET, "", true, true)
        .await?
        .send()
        .await?;

    match resp.status() {
        StatusCode::OK => Ok(resp.json::<SingleProjectDTO>().await?),
        _ => panic!("Not able to retrieve client's project."),
    }
}
