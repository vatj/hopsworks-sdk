use color_eyre::Result;
use log::debug;
use reqwest::{Method, StatusCode};
use super::{
    {ProjectAndUserDTO, SingleProjectDTO},
    payloads::NewProjectPayload,
};
use crate::get_hopsworks_client;

pub async fn get_project_and_user_list() -> Result<Vec<ProjectAndUserDTO>> {
    debug!("get_project_and_user_list");
    let builder = get_hopsworks_client()
        .await
        .request(Method::GET, "project", true, false)
        .await?;
    debug!("get_project_and_user_list builder: {:#?}", builder);
    let response = builder.send().await?;

    debug!("get_project_and_user_list response status: {:?}", response.status());
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

pub async fn get_client_project() -> Result<SingleProjectDTO> {
    let resp = get_hopsworks_client()
        .await
        .request(Method::GET, "", true, true)
        .await?
        .send()
        .await?;

    match resp.status() {
        StatusCode::OK => Ok(resp.json::<SingleProjectDTO>().await?),
        _ => Err(color_eyre::eyre::eyre!(
            "get_client_project failed with status : {:?}, here is the response :\n{:?}",
            resp.status(),
            resp.text_with_charset("utf-8").await?
        )),
    }
}
