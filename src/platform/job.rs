//! Manage and Create Jobs on Hopsworks Cluster
use color_eyre::Result;
use serde::{Deserialize, Serialize};

use crate::{
    core::platform::job_execution::start_new_execution_for_named_job,
    repositories::platform::job::JobDTO,
};

use super::job_execution::Execution;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Job {
    href: String,
    id: i32,
    name: String,
    creation_time: String,
    job_type: String,
}

impl Job {
    fn new_from_dto(job_dto: JobDTO) -> Self {
        Self {
            href: job_dto.href,
            id: job_dto.id,
            name: job_dto.name,
            creation_time: job_dto.creation_time,
            job_type: job_dto.job_type,
        }
    }
}

impl From<JobDTO> for Job {
    fn from(job_dto: JobDTO) -> Self {
        Self::new_from_dto(job_dto)
    }
}

impl Job {
    pub async fn run(&self) -> Result<Execution> {
        Ok(Execution::from(
            start_new_execution_for_named_job(self.name.as_str()).await?,
        ))
    }

    pub async fn get_executions(&self) -> Result<Vec<Execution>> {
        match crate::core::platform::job_execution::get_job_executions(self.name.as_str()).await {
            Ok(executions) => Ok(executions.into_iter().map(Execution::from).collect()),
            Err(e) => Err(e),
        }
    }

    pub async fn save(&self) -> Result<Job> {
        match crate::core::platform::job::update_job(
            self.name.as_str(),
            serde_json::json!({
                "job_type": self.job_type,
            }),
        )
        .await
        {
            Ok(job_dto) => Ok(Job::from(job_dto)),
            Err(e) => Err(e),
        }
    }

    pub async fn delete(&self) -> Result<()> {
        match crate::core::platform::job::delete_job(self.name.as_str()).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// Get template job configuration for a given job type.
///
/// # Arguments
///     * `job_type` - The type of job to get the configuration for.
///
/// # Returns
///    * `Result<serde_json::Value>` - The job configuration.
///
/// # Example
/// ```no_run
/// use color_eyre::Result;
/// use serde_json::json;
/// use hopsworks_rs::{login, platform::job::get_job_configuration};
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///    hopsowrks_rs::login(None).await?;
///
///    let default_pyspark_job_config = get_job_configuration("PYSPARK").await?;
///    println!("{#:?}", default_pyspark_job_config);
///
///   Ok(())
/// }
/// ```
pub async fn get_job_configuration(job_type: &str) -> Result<serde_json::Value> {
    crate::core::platform::job::get_job_configuration(job_type).await
}

/// Create a new job.
///
/// # Arguments
///    * `job_name` - The name of the job to create. This name must be unique.
///   * `job_configuration` - The configuration of the job to create.
///
/// # Returns
///   * `Result<Job>` - The created job.
///
/// # Example
/// ```no_run
/// use color_eyre::Result;
/// use serde_json::json;
/// use hopsworks_rs::{login, platform::job::{create_job, get_job_configuration}};
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///   hopsowrks_rs::login(None).await?;
///
///   let mut default_pyspark_job_config = get_job_configuration("PYSPARK").await?;
///   default_pyspark_job_config["appPath"] = "my-pyspark-job"
///   
///   let job = create_job("my-pyspark-job", default_pyspark_job_config).await?;
///   job.run().await?;
///
///   Ok(())
/// }
/// ```
pub async fn create_job(job_name: &str, job_configuration: serde_json::Value) -> Result<Job> {
    match crate::core::platform::job::create_job(job_name, job_configuration).await {
        Ok(job_dto) => Ok(Job::from(job_dto)),
        Err(e) => Err(e),
    }
}
