pub mod api;
pub(crate) mod clients;
pub(crate) mod domain;
pub(crate) mod kafka_producer;
pub(crate) mod repositories;
pub(crate) mod util;
pub use clients::rest_client::HopsworksClientBuilder;

use api::platform::project::Project;
use clients::rest_client::HopsworksClient;
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

/// Login to Hopsworks and return the chosen project.
/// If no client builder is provided, a default client builder to connect to https://app.hopsworks.ai is used.
///
/// # Requirements
/// You must provide an API key to login into Hopsworks either via the `HOPSWORKS_API_KEY`
/// environment variable or via the `api_key` field in the client builder. Login will panic if
/// no API key is provided.
///
/// # Example
/// ```no_run
/// use color_eyre::Result;
/// use hopsworks::hopsworks_login;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///    // The api key will be read from the environment variable HOPSWORKS_API_KEY
///    let project = hopsworks_login(None).await?;
///    Ok(())
/// }
/// ```
///
/// # Example with custom client builder
/// ```no_run
/// use color_eyre::Result;
/// use hopsworks::{hopsworks_login, HopsworksClientBuilder};
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///   let api_key = std::env::var("CUSTOM_API_KEY_ENV_VAR").unwrap();
///   let my_hopsworks_domain = "https://my-hopsworks-domain.com";
///   let builder = HopsworksClientBuilder::new(my_hopsworks_domain)
///      .api_key(api_key);
///
///   let project = hopsworks_login(Some(builder)).await?;
///   Ok(())
/// }
/// ```
///
/// # Panics
/// If no API key is provided via the `HOPSWORKS_API_KEY` environment variable or via the `api_key` field in the client builder.
pub async fn hopsworks_login(client_builder: Option<HopsworksClientBuilder>) -> Result<Project> {
    info!("Attempting to login to Hopsworks.");
    HOPSWORKS_CLIENT
        .get_or_try_init(|| async { client_builder.unwrap_or_default().build().await })
        .await?
        .login()
        .await
}
