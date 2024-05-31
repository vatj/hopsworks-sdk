use color_eyre::Result;
use reqwest::Method;

use hopsworks_base::get_hopsworks_client;
use super::entities::StringMessageResponse;

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

pub async fn get_loadbalancer_external_domain() -> Result<String> {
    Ok(get_hopsworks_client()
        .await
        .request(
            Method::GET,
            "variables/loadbalancer_external_domain",
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
