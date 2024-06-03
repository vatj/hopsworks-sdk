use color_eyre::Result;
use reqwest::Method;

use crate::get_hopsworks_client;
use super::CredentialsDTO;

pub async fn get_hopsworks_credentials_for_project() -> Result<CredentialsDTO> {
    Ok(get_hopsworks_client()
        .await
        .request(Method::GET, "credentials", true, true)
        .await?
        .send()
        .await?
        .json::<CredentialsDTO>()
        .await?)
}
