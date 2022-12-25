use color_eyre::Result;

use crate::get_hopsworks_client;

use super::entities::CredentialsDTO;

pub async fn get_hopsworks_credentials_for_project() -> Result<CredentialsDTO> {
    Ok(get_hopsworks_client()
        .await
        .send_get("credentials", true)
        .await?
        .json::<CredentialsDTO>()
        .await?)
}
