//! Manage and Create Jobs on Hopsworks Cluster
use color_eyre::Result;
use serde::{Deserialize, Serialize};

use crate::controller::platform::job_execution::start_new_execution_for_named_job;

use super::job_execution::JobExecution;
use hopsworks_internal::platform::job::JobDTO;
/// Job on Hopsworks Cluster. A job is akin to a script which can be executed on the cluster.
/// Jobs can be of different types, e.g. PySpark, Spark, Python, etc.
/// Jobs can be executed on the cluster and the [`JobExecution`] can be monitored.
///
/// > **_NOTE:_** Custom Jobs are only available in Hopsworks Enterprise Edition. In particular,
/// > [Hopsworks Serverless App](https://app.hopsworks.ai) does not support Custom Jobs.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Job {
    href: String,
    id: i32,
    name: String,
    creation_time: String,
    job_type: String,
    configuration: serde_json::Value,
}

impl Job {
    fn new_from_dto(job_dto: JobDTO) -> Self {
        Self {
            href: job_dto.href,
            id: job_dto.id,
            name: job_dto.name,
            creation_time: job_dto.creation_time,
            job_type: job_dto.job_type,
            configuration: job_dto.config,
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn job_type(&self) -> &str {
        self.job_type.as_str()
    }

    pub fn creation_time(&self) -> &str {
        self.creation_time.as_str()
    }

    pub fn href(&self) -> &str {
        self.href.as_str()
    }
}

impl From<JobDTO> for Job {
    fn from(job_dto: JobDTO) -> Self {
        Self::new_from_dto(job_dto)
    }
}

impl Job {
    /// Start a new [`JobExecution`] of this [`Job`], potentially waiting for it to finish.
    ///
    /// # Arguments
    /// * `await_termination` - Whether to wait for the execution to finish.
    ///
    /// # Returns
    /// * `Result<JobExecution>` - The started (or terminated) execution.
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    ///
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///   let project = hopsworks::login(None).await?;
    ///   let job = project.get_job("my_backfilling_job").await?;
    ///   let job_exec = job.run(None, true).await?;
    ///
    ///   // Check execution state
    ///   println!("{:?}", job_exec.get_state());
    ///   Ok(())
    /// }
    /// ```
    pub async fn run(&self, args: Option<&str>, await_termination: bool) -> Result<JobExecution> {
        let exec =
            JobExecution::from(start_new_execution_for_named_job(self.name.as_str(), args).await?);
        if await_termination {
            exec.await_termination().await?;
        }
        Ok(exec)
    }

    /// Get the [`JobExecution`]s of this job sorted by submission date starting with most recent.
    ///
    /// # Returns
    /// * `Result<Vec<JobExecution>>` - The [`JobExecution`]s of this job.
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    /// use hopsworks::platform::job_execution::JobExecutionState;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///  let project = hopsworks::login(None).await?;
    ///  let job = project.get_job("my_backfilling_job").await?;
    ///  let executions = job.get_executions().await?;
    ///  println!("Most recent failed executions {:?}",
    ///    executions.iter().find(|e| e.get_state() == JobExecutionState::Failed).expect("No failed executions found")
    ///    .get_submission_time());
    ///  Ok(())
    /// }
    /// ```
    pub async fn get_executions(&self) -> Result<Vec<JobExecution>> {
        match crate::controller::platform::job_execution::get_job_executions(self.name.as_str()).await {
            Ok(executions) => Ok(executions.into_iter().map(JobExecution::from).collect()),
            Err(e) => Err(e),
        }
    }

    /// Update the job configuration. This will not affect running [`JobExecution`]s of this job.
    ///
    /// # Returns
    /// * `Result<Job>` - The updated job.
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    ///
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///   let project = hopsworks::login(None).await?;
    ///   let job = project.get_job("my_backfilling_job").await?;
    ///
    ///   let mut job_config = job.get_configuration();
    ///   job_config["appPath"] = "my-new-pyspark-job".into();
    ///   job.save(job_config).await?;
    ///   
    ///   Ok(())
    /// }
    /// ```
    pub async fn save(&self, updated_job_config: serde_json::Value) -> Result<Job> {
        match crate::controller::platform::job::update_job(self.name.as_str(), updated_job_config).await {
            Ok(job_dto) => Ok(Job::from(job_dto)),
            Err(e) => Err(e),
        }
    }

    /// Delete the [`Job`]. This will terminate running [`JobExecution`]s.
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    ///
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///   let project = hopsworks::login(None).await?;
    ///   let job = project.get_job("my_backfilling_job").await?;
    ///   job.delete().await?;
    ///
    ///   Ok(())
    /// }
    /// ```
    pub async fn delete(&self) -> Result<()> {
        match crate::controller::platform::job::delete_job(self.name.as_str()).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Get the job configuration.
    ///
    /// # Returns
    /// * `serde_json::Value` - A clone of the job configuration.
    pub fn get_configuration(&self) -> serde_json::Value {
        self.configuration.clone()
    }
}

/// Get template job configuration for a given job type.
///
/// # Arguments
/// * `job_type` - The job type to get the configuration for.
///
/// # Returns
/// * `Result<serde_json::Value>` - The job configuration.
///
/// # Example
/// ```no_run
/// use color_eyre::Result;
/// use serde_json::json;
/// use hopsworks::platform::job::get_job_configuration;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///    hopsworks::login(None).await?;
///
///    let default_pyspark_job_config = get_job_configuration("PYSPARK").await?;
///    println!("{}", default_pyspark_job_config);
///
///   Ok(())
/// }
/// ```
pub async fn get_job_configuration(job_type: &str) -> Result<serde_json::Value> {
    crate::controller::platform::job::get_job_configuration(job_type).await
}

/// Create a new job.
///
/// # Arguments
///   * `job_name` - The name of the job to create. This name must be unique.
///   * `job_configuration` - The configuration of the job to create.
///
/// # Returns
///   * `Result<Job>` - The created job.
///
/// # Example
/// ```no_run
/// use color_eyre::Result;
/// use serde_json::json;
/// use hopsworks::platform::job::{create_job, get_job_configuration};
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///   hopsworks::login(None).await?;
///
///   let mut default_pyspark_job_config = get_job_configuration("PYSPARK").await?;
///   default_pyspark_job_config["appPath"] = "my-pyspark-job".into();
///   
///   let job = create_job("my-pyspark-job", default_pyspark_job_config).await?;
///   job.run(None, false).await?;
///
///   Ok(())
/// }
/// ```
pub async fn create_job(job_name: &str, job_configuration: serde_json::Value) -> Result<Job> {
    match crate::controller::platform::job::create_job(job_name, job_configuration).await {
        Ok(job_dto) => Ok(Job::from(job_dto)),
        Err(e) => Err(e),
    }
}
