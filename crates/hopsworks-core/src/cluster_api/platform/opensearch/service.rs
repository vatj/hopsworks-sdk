use color_eyre::Result;
use reqwest::Method;

use crate::get_hopsworks_client;

use super::OpenSearchTokenDTO;

pub async fn get_authorization_token(project_id: i32) -> Result<String> {
    let resp = get_hopsworks_client()
        .await
        .request(Method::GET, format!("elastic/jwt/{}", project_id).as_str(), true, false)
        .await?
        .send()
        .await?;

    if resp.status().is_success() {
        Ok(resp.json::<OpenSearchTokenDTO>().await.map(|dto| dto.token)?)
    } else {
        Err(resp.error_for_status().unwrap_err().into())
    }
}