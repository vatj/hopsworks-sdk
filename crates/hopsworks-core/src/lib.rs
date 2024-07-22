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
use log::debug;
use tokio::sync::OnceCell;
use std::sync::{Arc, OnceLock};

pub(crate) mod cluster_api;
pub mod controller;
pub mod feature_store;
pub mod platform;
pub mod profiles;
pub mod util;
pub mod rest_client;

pub use platform::project::Project;
pub use rest_client::HopsworksClientBuilder;
use rest_client::HopsworksClient;


static NUM_LOGICAL_CPUS: OnceCell<usize> = OnceCell::const_new();
static THREADED_RUNTIME_NUM_WORKER_THREADS: OnceCell<usize> = tokio::sync::OnceCell::const_new();
static THREADED_RUNTIME: OnceLock<Arc<tokio::runtime::Runtime>> = OnceLock::new();
static SINGLE_THREADED_RUNTIME: OnceLock<Arc<tokio::runtime::Runtime>> = OnceLock::new();
static HOPSWORKS_CLIENT: OnceLock<HopsworksClient> = OnceLock::new();

pub fn get_logical_cpus() -> usize {
    if NUM_LOGICAL_CPUS.initialized() {
        *NUM_LOGICAL_CPUS.get().unwrap()
    } else {
        let env_requested_threads = std::env::var("HOPSWORKS_NUM_THREADS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok());
        let num_logical_cpus = env_requested_threads.unwrap_or(std::thread::available_parallelism().unwrap().get());
        tracing::debug!("Detected {} logical CPUs", num_logical_cpus);
        NUM_LOGICAL_CPUS.set(num_logical_cpus).unwrap();
        num_logical_cpus
    }
}

pub fn get_threaded_runtime_num_worker_threads() -> usize {
    if THREADED_RUNTIME_NUM_WORKER_THREADS.initialized() {
        *THREADED_RUNTIME_NUM_WORKER_THREADS.get().unwrap()
    } else {
        let num_worker_threads = get_logical_cpus();
        THREADED_RUNTIME_NUM_WORKER_THREADS.set(2 * num_worker_threads).unwrap();
        num_worker_threads
    }
}

pub fn get_threaded_runtime() -> &'static Arc<tokio::runtime::Runtime> {
    if THREADED_RUNTIME.get().is_some() {
        THREADED_RUNTIME.get().unwrap()
    } else {
        let num_worker_threads = get_threaded_runtime_num_worker_threads();
        THREADED_RUNTIME.set(
            Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                .worker_threads(num_worker_threads)
                .enable_all()
                .build()
                .unwrap()
            )).unwrap();
        tracing::debug!("Initialized multi-threaded runtime with {} worker threads", num_worker_threads);
        THREADED_RUNTIME.get().unwrap()
    }
}

pub fn get_single_threaded_runtime() -> &'static Arc<tokio::runtime::Runtime> {
    if SINGLE_THREADED_RUNTIME.get().is_some() {
        SINGLE_THREADED_RUNTIME.get().unwrap()
    } else {
        SINGLE_THREADED_RUNTIME.set(
            Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap()
            )).unwrap();
        SINGLE_THREADED_RUNTIME.get().unwrap()
    }
}

pub fn get_hopsworks_runtime(multithreaded: bool) -> &'static Arc<tokio::runtime::Runtime> {
    if multithreaded {
        get_threaded_runtime()
    } else {
        get_single_threaded_runtime()
    }
}

pub async fn get_hopsworks_client() -> &'static HopsworksClient {
    debug!("Access global Hopsworks Client");
    match HOPSWORKS_CLIENT.get() {
        Some(client) => client,
        None => panic!(
            "First use hopsworks::login() to initialize the Hopsworks client with your credentials."
        ),
    }
}

pub async fn login(client_builder: Option<HopsworksClientBuilder>, multithreaded: bool) -> Result<Project> {
    if HOPSWORKS_CLIENT.get().is_some() {
        Err(color_eyre::eyre::eyre!("Hopsworks client already initialized"))
    }
    else {
        let rt = get_hopsworks_runtime(multithreaded).clone();
        let _guard = rt.enter();

        let client = client_builder.unwrap_or_default().build().await?;
        let init_client = HOPSWORKS_CLIENT.get_or_init(|| client);
        let project_dto = init_client.login().await?;
        Ok(Project::from(&project_dto))
    } 
}