pub mod api;
pub mod clients;
pub mod domain;
pub mod kafka_producer;
pub mod minidf;
pub mod repositories;
pub mod util;

use api::project::entities::Project;
use clients::arrow_flight_client::{HopsworksArrowFlightClient, HopsworksArrowFlightClientBuilder};
use clients::rest_client::{HopsworksClient, HopsworksClientBuilder};
use color_eyre::Result;
use log::{debug, info};
use tokio::sync::OnceCell;

static HOPSWORKS_CLIENT: OnceCell<HopsworksClient> = OnceCell::const_new();
static HOPSWORKS_ARROW_FLIGHT_CLIENT: OnceCell<HopsworksArrowFlightClient> = OnceCell::const_new();

async fn get_hopsworks_client() -> &'static HopsworksClient {
    debug!("Access global Hopsworks Client");
    match HOPSWORKS_CLIENT.get() {
        Some(client) => client,
        None => panic!(
            "First use hopsworks_login to initialize the Hopsworks client with your credentials."
        ),
    }
}

pub async fn hopsworks_login(client_builder: Option<HopsworksClientBuilder>) -> Result<Project> {
    info!("Attempting to login to Hopsworks.");
    HOPSWORKS_CLIENT
        .get_or_try_init(|| async { client_builder.unwrap_or_default().build().await })
        .await?
        .login()
        .await
}

pub async fn get_hopsworks_arrow_flight_client() -> Result<&'static HopsworksArrowFlightClient> {
    debug!("Access global Hopsworks Arrow Flight Client");
    HOPSWORKS_ARROW_FLIGHT_CLIENT
        .get_or_try_init(|| async { HopsworksArrowFlightClientBuilder::default().build().await })
        .await
}
