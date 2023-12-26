//! # Hopsworks SDK for Rust
//!
//! The `hopsworks-rs` crate is a Rust SDK to interact with the Hopsworks Platform and [`FeatureStore`][feature_store::FeatureStore]. It is intended
//! to be used in conjunction with a [Hopsworks](https://www.hopsworks.ai/) cluster to build end-to-end machine
//! learning pipelines. With Hopsworks you can:
//! - Schedule Feature Engineering Jobs to ingest data from various sources into the Feature Store
//! - Stream data into the Feature Store for real-time applications
//! - Re-use Features across teams and projects
//! - Create training datasets by joining Features to train your models
//! - Serve Feature vectors as batches or in real-time to your models
//! - Define transformations to apply to the raw data before serving it to the model
//!
//! The SDK is currently in alpha and is under active development and therefore not suitable for production use cases.
//! Only a subset of the functionalities offered by a [Hopsworks](https://www.hopsworks.ai/) cluster are currently implemented,
//! check out the [Python and Java SDK](https://github.com/logicalclocks/hopsworks-api) if you need a more complete implementation.
//! It is a community project and we welcome any feedback or contributions.
//!
//! ## Prerequisites
//!
//! To work with Hopsworks-rs you need to connect to a [Hopsworks](https://www.hopsworks.ai/) cluster.
//! Quickest way to get started is to register for free to use the [Hopsworks Serverless App](https://app.hopsworks.ai).
//! Once you are registered you can create a project and generate an API key to enable the SDK to connect to your project.
//! Either copy the config template and paste your API key or export it as an environment variable to enable the SDK
//! to connect to your project.
//!
//! ## Quickstart
//!
//! Check out the examples folder to see how to use the SDK to build end-to-end machine learning pipelines.
//!
//! ### Connect to Hopsworks Serverless App
//! ```no_run
//! use color_eyre::Result;
//! use polars::prelude::*;
//! use hopsworks_rs::hopsworks_login;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!  // The api key will be read from the environment variable HOPSWORKS_API_KEY
//!  let project = hopsworks_login(None).await?;
//!  // Get the default feature store for the project
//!  let fs = project.get_feature_store().await?;
//!
//!  // Create a new feature group and ingest local data to the Feature Store
//!  let mut df = CsvReader::from_path("./examples/data/transactions.csv")?.finish()?;
//!  let mut fg = fs.create_feature_group(
//!    "my_fg",
//!    1,
//!    None,
//!    vec!["primary_key_feature_name(s)"],
//!    Some("event_time_feature_name"),
//!    false
//!  )?;
//!  fg.insert(&mut df).await?;
//!
//!  // Create a feature view to read data from the Feature Store,
//!  // see Feature View page for more complex examples
//!  let fv = fs.create_feature_view(
//!    "my_fv",
//!    1,
//!    fg.select(&["feature1", "feature2"])?,
//!    None,
//!  ).await?;
//!
//!  // Read data from the Feature View
//!  let df = fv.read_from_offline_feature_store(None).await?;
//!  
//!  Ok(())
//! }
//! ```
//!
//! ## Connect to a different Hopsworks Cluster
//!
//! ```no_run
//! # use color_eyre::Result;
//! use hopsworks_rs::{hopsworks_login, HopsworksClientBuilder};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!  let api_key = std::env::var("HOPSWORKS_API_KEY").unwrap();
//!  let my_hopsworks_domain = "https://my-hopsworks-domain.com";
//!  let builder = HopsworksClientBuilder::new()
//!    .with_url(my_hopsworks_domain)
//!    .with_api_key(&api_key);
//!
//!  let project = hopsworks_login(Some(builder)).await?;
//!  let fs = project.get_feature_store().await?;
//!  Ok(())
//! }
//! ```

pub(crate) mod clients;
pub(crate) mod core;
pub(crate) mod kafka_producer;
pub(crate) mod repositories;
pub(crate) mod util;

pub use feature_store::{FeatureGroup, FeatureStore, FeatureView};

pub mod feature_store;
pub mod platform;

pub use clients::rest_client::HopsworksClientBuilder;

use clients::rest_client::HopsworksClient;
use color_eyre::Result;
use log::{debug, info};
use platform::project::Project;
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
/// If no client builder is provided, a default client builder to connect to [Hopsworks Serverless App](https://app.hopsworks.ai) is used.
///
/// # Requirements
/// You must provide an API key to login into Hopsworks either via the `HOPSWORKS_API_KEY`
/// environment variable or via the `api_key` field in the client builder. Login will panic if
/// no API key is provided.
///
/// # Example
/// ```no_run
/// use color_eyre::Result;
/// use hopsworks_rs::hopsworks_login;
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
/// use hopsworks_rs::{hopsworks_login, HopsworksClientBuilder};
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///   let api_key = std::env::var("CUSTOM_API_KEY_ENV_VAR").unwrap();
///   let my_hopsworks_domain = "https://my-hopsworks-domain.com";
///   let builder = HopsworksClientBuilder::new()
///      .with_api_key(&api_key)
///      .with_url(my_hopsworks_domain);
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
