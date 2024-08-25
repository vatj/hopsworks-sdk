use color_eyre::Result;
use reqwest::Method;

use crate::get_hopsworks_client;
use super::StringMessageResponse;

pub async fn get_flyingduck_enabled() -> Result<bool> {
    Ok(get_hopsworks_client()
        .await
        .request(Method::GET, "variables/enable_flyingduck", true, false)
        .await?
        .send()
        .await?
        .json::<StringMessageResponse>()
        .await?
        .success_message
        == "true")
}

pub async fn get_loadbalancer_external_domain(service: &str) -> Result<String> {
    let path = format!("variables/loadbalancer_external_domain_{}", service);
    Ok(get_hopsworks_client()
        .await
        .request(
            Method::GET,
            &path,
            true,
            false,
        )
        .await?
        .send()
        .await?
        .json::<StringMessageResponse>()
        .await?
        .success_message)
}
