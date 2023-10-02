pub mod api;
pub mod client;
pub mod domain;
pub mod kafka_producer;
pub mod minidf;
pub mod repositories;
pub mod arrow_flight_client;
pub mod util;

use api::project::entities::Project;
use arrow_flight_client::HopsworksArrowFlightClient;
use client::HopsworksClient;
use color_eyre::Result;
use log::{debug, info};
use tokio::sync::OnceCell;

use crate::arrow_flight_client::HopsworksArrowFlightClientBuilder;

static HOPSWORKS_CLIENT: OnceCell<HopsworksClient> = OnceCell::const_new();
static HOPSWORKS_ARROW_FLIGHT_CLIENT: OnceCell<HopsworksArrowFlightClient> = OnceCell::const_new();

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

pub async fn get_hopsworks_arrow_flight_client() -> Result<&'static HopsworksArrowFlightClient> {
    debug!("Access global Hopsworks Arrow Flight Client");
    HOPSWORKS_ARROW_FLIGHT_CLIENT
        .get_or_try_init(|| async { HopsworksArrowFlightClientBuilder::default().build().await })
        .await
}