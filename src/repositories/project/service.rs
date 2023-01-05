use color_eyre::Result;
use log::info;
use reqwest::StatusCode;

use super::{
    entities::{ProjectAndUserDTO, SingleProjectDTO},
    payloads::NewProjectPayload,
};
use crate::get_hopsworks_client;

pub async fn get_project_and_user_list() -> Result<Vec<ProjectAndUserDTO>> {
    Ok(get_hopsworks_client()
        .await
        .send_get("project", false)
        .await?
        .json::<Vec<ProjectAndUserDTO>>()
        .await?)
}

pub async fn create_project(
    new_project_payload: &NewProjectPayload<'_>,
) -> Result<Vec<ProjectAndUserDTO>> {
    Ok(get_hopsworks_client()
        .await
        .send_post_json("project", new_project_payload, false)
        .await?
        .json::<Vec<ProjectAndUserDTO>>()
        .await?)
}

pub async fn get_client_project() -> Result<SingleProjectDTO> {
    let resp = get_hopsworks_client().await.send_get("", true).await?;

    match resp.status() {
        StatusCode::OK => Ok(resp.json::<SingleProjectDTO>().await?),
        _ => panic!("Not able to retrieve client's project."),
    }
}
