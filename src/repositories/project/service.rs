use color_eyre::Result;

use super::entities::ProjectAndUserDTO;
use crate::get_hopsworks_client;

pub async fn get_project_and_user_list() -> Result<Vec<ProjectAndUserDTO>> {
    Ok(get_hopsworks_client()
        .await
        .send_get("project")
        .await?
        .json::<Vec<ProjectAndUserDTO>>()
        .await?)
}
