use color_eyre::Result;

use crate::get_hopsworks_client;

use super::entities::CredentialsDTO;

pub async fn get_hopsworks_credentials_for_project(project_id: i32) -> Result<CredentialsDTO> {
    Ok(get_hopsworks_client()
        .await
        .send_get(format!("project/{project_id}/credentials").as_str())
        .await?
        .json::<CredentialsDTO>()
        .await?)
}
