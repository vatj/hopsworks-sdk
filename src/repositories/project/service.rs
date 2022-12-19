use color_eyre::Result;

use super::entities::{ProjectAndUserDTO, ProjectDTO};
use crate::get_hopsworks_client;

pub async fn get_project_list() -> Result<Vec<ProjectDTO>> {
    let projects_and_users: Vec<ProjectAndUserDTO> = get_hopsworks_client()
        .await
        .send_get("project")
        .await?
        .json()
        .await?;

    Ok(projects_and_users
        .iter()
        .map(|project_and_user| project_and_user.project.to_owned())
        .collect())
}
