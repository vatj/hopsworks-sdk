use color_eyre::Result;
use reqwest::Method;

use crate::get_hopsworks_client;

use super::entities::{BoolMessageResponse, StringMessageResponse};

pub async fn get_flyingduck_enabled() -> Result<bool> {
    Ok(get_hopsworks_client()
            .await
            .request(Method::GET, "variables/enable_flying_duck", true, true)
            .await?
            .send()
            .await?
            .json::<BoolMessageResponse>()
            .await?
            .success_message)
}

pub async fn get_loadbalancer_external_domain() -> Result<String> {
    Ok(get_hopsworks_client()
            .await
            .request(Method::GET, "variables/loadbalancer_external_domain", true, true)
            .await?
            .send()
            .await?
            .json::<StringMessageResponse>()
            .await?
            .success_message)
}