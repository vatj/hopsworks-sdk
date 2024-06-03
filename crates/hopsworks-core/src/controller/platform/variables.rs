use color_eyre::Result;

use crate::cluster_api::platform::variables::service;

pub async fn get_flyingduck_enabled() -> Result<bool> {
    service::get_flyingduck_enabled().await
}

pub async fn get_loadbalancer_external_domain() -> Result<String> {
    service::get_loadbalancer_external_domain().await
}