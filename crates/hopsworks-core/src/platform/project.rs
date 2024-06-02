//! Project with Feature Store and Other Platform Resources
//!
//! The [`Project`] is the top-level entity in Hopsworks. With its own [`FeatureStore`],
//! it is intended to hold multiple [`FeatureGroup`][crate::feature_store::FeatureGroup]s and
//! [`FeatureView`][crate::feature_store::FeatureView]s, the [`Job`]s to backfill
//! or create [`TrainingDataset`][crate::feature_store::feature_view::training_dataset::TrainingDataset]s, managing [`User`][super::user::User]s access, etc...
use color_eyre::Result;
use serde::{Deserialize, Serialize};

use crate::core::feature_store;
use crate::feature_store::FeatureStore;
use crate::hopsworks_internal::platform::project::ProjectDTO;

use super::job::Job;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Project {
    project_name: String,
    id: i32,
}

impl From<&ProjectDTO> for Project {
    fn from(project_dto: &ProjectDTO) -> Self {
        Self {
            project_name: project_dto.name.clone(),
            id: project_dto.id,
        }
    }
}

impl Project {
    /// Get the name of the project.
    pub fn name(&self) -> &str {
        self.project_name.as_str()
    }

    /// Get the id of the project.
    pub fn id(&self) -> i32 {
        self.id
    }

    /// Get the default [`FeatureStore`] for the project. Use it once the connection is established to start
    /// managing the Feature Store, e.g. creating/updating Feature Groups and Feature Views, inserting or reading
    /// feature data.
    ///
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///   let project = hopsworks::login(None).await?;
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

    /// Get a [`Job`] by name. Use it to manage the job, e.g. run it or update the configuration.
    ///
    /// # Arguments
    /// * `job_name` - The name of the job.
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    ///
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///  let project = hopsworks::login(None).await?;
    ///  let job = project.get_job("my_job").await?;
    ///
    ///  let mut job_config = job.get_configuration();
    ///  job_config["driverCores"] = serde_json::Value::from(2);
    ///  job.save(job_config).await?;
    ///  job.run(None, false).await?;
    ///  
    ///  Ok(())
    /// }
    /// ```
    pub async fn get_job(&self, job_name: &str) -> Result<Job> {
        crate::core::platform::job::get_job_by_name(job_name).await
    }

    /// Get a list of all [`Job`]s in the project.
    /// Use it to list all jobs in the project and manage them, e.g. get a job by name, run it, or update the configuration.
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///   let project = hopsworks::login(None).await?;
    ///   let jobs = project.get_jobs().await?;
    ///
    ///   jobs.iter().foreach(|job| println!("Job: {:#?}", job))
    ///  Ok(())
    /// }
    /// ```
    pub async fn get_jobs(&self) -> Result<Vec<Job>> {
        crate::core::platform::job::get_job_list().await
    }
}

pub async fn create_project(project_name: &str, description: &Option<&str>) -> Result<Project> {
    crate::core::platform::project::create_project(project_name, description).await
}

pub async fn get_project_list() -> Result<Vec<Project>> {
    Ok(crate::core::platform::project::get_project_list()
        .await?
        .iter()
        .map(Project::from)
        .collect())
}
