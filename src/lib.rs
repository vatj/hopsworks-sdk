pub mod api;
pub mod client;
pub mod domain;
pub mod kafka_producer;
pub mod minidf;
pub mod repositories;

use api::project::entities::Project;
use client::HopsworksClient;
use color_eyre::Result;
use log::{debug, info};
use tokio::sync::OnceCell;

static HOPSWORKS_CLIENT: OnceCell<HopsworksClient> = OnceCell::const_new();

async fn get_hopsworks_client() -> &'static HopsworksClient {
    debug!("Access global Hopsworks Client");
    HOPSWORKS_CLIENT
        .get_or_init(|| async { HopsworksClient::default() })
        .await
}

pub async fn hopsworks_login() -> Result<Project> {
    info!("Login with Hopsworks Client");
    get_hopsworks_client().await.login().await
}
