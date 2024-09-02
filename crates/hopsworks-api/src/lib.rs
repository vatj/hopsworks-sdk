//! # Hopsworks SDK for Rust
//!
//! The `hopsworks-rs` crate is a Rust SDK to interact with the Hopsworks Platform and [`FeatureStore`]. It is intended
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
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!  // The api key will be read from the environment variable HOPSWORKS_API_KEY
//!  let project = hopsworks::login(None).await?;
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
//! use hopsworks::HopsworksClientBuilder;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!  let api_key = std::env::var("HOPSWORKS_API_KEY").unwrap();
//!  let my_hopsworks_domain = "https://my-hopsworks-domain.com";
//!  let builder = HopsworksClientBuilder::new()
//!    .with_url(my_hopsworks_domain)
//!    .with_api_key(&api_key);
//!
//!  let project = hopsworks::login(Some(builder)).await?;
//!  let fs = project.get_feature_store().await?;
//!  Ok(())
//! }
//! ```
use color_eyre::Result;

pub use hopsworks_core::feature_store::{
    embedding::EmbeddingFeature,
    embedding::EmbeddingIndex,
    feature_view::training_dataset::TrainingDataset,
    query::{builder::BatchQueryOptions, Query},
    FeatureGroup, FeatureStore, FeatureView,
};
pub use hopsworks_core::platform::{job::Job, job_execution::JobExecution, project::Project};

#[cfg(feature = "insert_into_kafka")]
pub mod kafka;
#[cfg(feature = "read_arrow_flight_offline_store")]
pub mod offline_store;
#[cfg(feature = "read_sql_online_store")]
pub mod online_store;

#[cfg(feature = "opensearch")]
pub mod opensearch;

#[cfg(feature = "blocking")]
pub mod blocking;

#[cfg(feature = "polars")]
pub mod polars;

pub use hopsworks_core::HopsworksClientBuilder;
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

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
///
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///    // The api key will be read from the environment variable HOPSWORKS_API_KEY
///    let project = hopsworks::login(None).await?;
///    Ok(())
/// }
/// ```
///
/// # Example with custom client builder
/// ```no_run
/// use color_eyre::Result;
/// use hopsworks::HopsworksClientBuilder;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///   let api_key = std::env::var("CUSTOM_API_KEY_ENV_VAR").unwrap();
///   let my_hopsworks_domain = "https://my-hopsworks-domain.com";
///   let builder = HopsworksClientBuilder::new()
///      .with_api_key(&api_key)
///      .with_url(my_hopsworks_domain);
///
///   let project = hopsworks::login(Some(builder)).await?;
///   Ok(())
/// }
/// ```
///
/// # Panics
/// If no API key is provided via the `HOPSWORKS_API_KEY` environment variable or via the `api_key` field in the client builder.
pub async fn login(
    client_builder: Option<HopsworksClientBuilder>,
    multithreaded: bool,
) -> Result<Project> {
    hopsworks_core::login(client_builder, multithreaded).await
}

#[cfg(feature = "blocking")]
pub fn login_blocking(
    client_builder: Option<HopsworksClientBuilder>,
    multithreaded: bool,
) -> Result<Project> {
    let rt = hopsworks_core::get_hopsworks_runtime(multithreaded);
    let _guard = rt.enter();

    rt.block_on(hopsworks_core::login(client_builder, multithreaded))
}
