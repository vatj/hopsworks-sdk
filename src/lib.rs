pub mod api;
pub mod clients;
pub(crate) mod domain;
pub(crate) mod kafka_producer;
pub(crate) mod repositories;
pub(crate) mod util;

use api::project::entities::Project;
use clients::rest_client::{HopsworksClient, HopsworksClientBuilder};
use color_eyre::Result;
use log::{debug, info};
use tokio::sync::OnceCell;

static HOPSWORKS_CLIENT: OnceCell<HopsworksClient> = OnceCell::const_new();

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
