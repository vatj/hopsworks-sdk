use color_eyre::Result;
use log::info;

pub mod controller;
pub mod feature_store;
pub mod platform;

pub use hopsworks_internal::HopsworksClientBuilder;
pub use platform::project::Project;

pub async fn login(client_builder: Option<HopsworksClientBuilder>) -> Result<Project> {
    info!("Attempting to login to Hopsworks.");
    Ok(hopsworks_internal::login(client_builder).await.map(|project_dto| Project::from(project_dto))?)
}