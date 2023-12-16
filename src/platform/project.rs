//! Project with Feature Store and Other Platform Resources
use color_eyre::Result;
use serde::{Deserialize, Serialize};

use crate::core::feature_store;
use crate::feature_store::FeatureStore;
use crate::repositories::platform::project::entities::ProjectDTO;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Project {
    pub project_name: String,
    pub id: i32,
}

impl From<ProjectDTO> for Project {
    fn from(project_dto: ProjectDTO) -> Self {
        Self {
            project_name: project_dto.name,
            id: project_dto.id,
        }
    }
}

impl Project {
    /// Get the default Feature Store for the project. Use it once the connection is established to start
    /// managing the Feature Store, e.g. creating/updating Feature Groups and Feature Views, inserting or reading
    /// feature data.
    ///
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    /// use hopsworks_rs::{hopsworks_login, HopsworksClientBuilder};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///   let project = hopsworks_login(None).await?;
    ///   let fs = project.get_feature_store().await?;
    ///
    ///   // Create/Update Feature Groups and Feature Views, Insert/Read Feature Data
    ///
    ///   Ok(())
    /// }
    /// ```
    pub async fn get_feature_store(&self) -> Result<FeatureStore> {
        Ok(FeatureStore::from(
            feature_store::get_project_default_feature_store(self.project_name.as_str()).await?,
        ))
    }
}
