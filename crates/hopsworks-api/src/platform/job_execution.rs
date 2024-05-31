use color_eyre::Result;
use serde::{Deserialize, Serialize};

use crate::cluster_api::platform::job_execution::JobExecutionDTO;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobExecutionUserDTO {
    href: String,
}

/// Represents a single job execution and enables its monitoring and management.
/// Note that similar functionalities are available through the Hopsworks UI
/// which can be better suited to some use cases.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobExecution {
    href: String,
    id: i32,
    job_name: String,
    state: JobExecutionState,
    submission_time: String,
}

impl JobExecution {
    fn new_from_dto(execution_dto: JobExecutionDTO) -> Self {
        Self {
            href: execution_dto.href,
            id: execution_dto.id,
            job_name: execution_dto
                .job_name
                .expect("Job name not provided in job execution DTO"),
            state: execution_dto.state.into(),
            submission_time: execution_dto.submission_time,
        }
    }
}

impl From<JobExecutionDTO> for JobExecution {
    fn from(execution_dto: JobExecutionDTO) -> Self {
        Self::new_from_dto(execution_dto)
    }
}

impl JobExecution {
    /// Download the logs of the job execution from the Hopsworks cluster to the local file system.
    /// The logs are downloaded to the current working directory if no local_dir is provided. Two log
    /// files are downloaded, one for stdout and one for stderr.
    ///
    /// Job execution logs are also available on the Hopsworks UI.
    ///
    /// > **_NOTE:_** The logs are only available after the job execution has terminated.
    ///
    /// # Arguments
    /// * `local_dir` - The local directory to download the logs to.
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    /// use hopsworks::platform::job_execution::JobExecutionState;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///   let project = hopsworks::login(None).await?;
    ///   let job = project.get_job("my_backfilling_job").await?;
    ///   let job_exec = job.run(None, true).await?;
    ///
    ///   if job_exec.get_state() == JobExecutionState::Failed {
    ///     job_exec.download_logs(Some("./logs/")).await?;
    ///   }
    ///
    ///   Ok(())
    /// }
    /// ```
    pub async fn download_logs(&self, local_dir: Option<&str>) -> Result<()> {
        crate::core::platform::job_execution::download_job_execution_logs(
            self.job_name.as_str(),
            self.id,
            local_dir,
        )
        .await
    }

    /// Delete the job execution from the hopsworks cluster. Note that if the job_execution is still running
    /// it will be terminated before deletion. The deletion cleans up the logs on the file system.
    pub async fn delete(&self) -> Result<()> {
        crate::core::platform::job_execution::delete_job_execution(self.job_name.as_str(), self.id)
            .await
    }

    /// Block a thread until the job execution on the hopsworks cluster is terminated. Note that this does not guarantee the job finished
    /// without error. Rather that the ressources allocated to the job execution are freed and logs are available.
    /// Some operations might be contingent on a job execution being terminated, e.g.
    /// waiting for the insertion of data to the feature store to be complete before creating a new up to date training dataset.
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    ///
    /// use polars::prelude::*;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///   let feature_store = hopsworks::login(None).await?
    ///     .get_feature_store().await?;
    ///
    ///   let mut feature_group = feature_store
    ///     .get_feature_group("my_feature_group", Some(1)).await?
    ///     .expect("Feature Group not found");
    ///   
    ///   let mut new_df = CsvReader::from_path("./examples/data/transactions.csv")?.finish()?;
    ///
    ///   let job_exec = feature_group.insert(&mut new_df).await?;
    ///   job_exec.await_termination().await?;
    ///
    ///   // Read new and old data via a feature view which contains features from the my_feature_group
    ///   let feature_view = feature_store
    ///     .get_feature_view("my_feature_view", Some(1)).await?
    ///     .expect("Feature View not found");
    ///
    ///   let updated_training_df = feature_view.read_from_offline_feature_store(None).await?;
    ///
    ///   // Do stuff with the updated training dataset
    ///
    ///   Ok(())
    /// }
    /// ```
    pub async fn await_termination(&self) -> Result<()> {
        crate::core::platform::job_execution::await_termination(self.job_name.as_str(), self.id)
            .await
    }

    /// Stop the execution of the job on the hopsworks cluster. Note that this does not delete the job execution.
    /// The job execution will transition to the state "STOPPED" and its final status will be set to "KILLED".
    /// The logs of the job execution will be available on the cluster, however this may take some time.
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    /// use std::time::SystemTime;
    /// use hopsworks::platform::job_execution::JobExecutionState;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///  let project = hopsworks::login(None).await?;
    ///
    ///  let job = project.get_job("job_with_an_execution_to_be_killed").await?;
    ///  let job_exec = job.run(None, false);
    ///
    ///  // complicated logic monitoring the job execution
    ///  job_exec_has_turned_rogue = (42 > 1);
    ///  
    ///  if job_exec_has_turned_rogue { stopped_exec = job_exec.stop() }
    ///
    ///  println("Job execution took a turn for the worst, it got {:?}.", stopped_exec.state())
    ///  
    ///  Ok(())
    /// ```
    pub async fn stop(&self) -> Result<JobExecution> {
        Ok(crate::core::platform::job_execution::stop_job_execution(
            self.job_name.as_str(),
            self.id,
        )
        .await?
        .into())
    }

    /// Get the current state of the job execution from the hopsworks cluster.
    /// The state of a job execution is one of [`JobExecutionState`].
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
    ///  let job_exec = job.run(None, false).await?;
    ///  # On triggering execution, the new object will have the state "Initializing"
    ///  println!("Job execution state: {:?}", job_exec.state());
    ///  
    ///  while let Ok(current_state) = job_exec.get_current_state().await? {
    ///    match current_state {
    ///      JobExecutionState::Initializing => {
    ///        println!("Job execution still initializing");
    ///        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    ///      },
    ///      JobExecutionState::Running => {
    ///        println!("Job execution is now running");
    ///        break;
    ///      },
    ///      _ => {
    ///       println!("Job execution is not running, transitioned to state: {:?}", current_state);
    ///       break;
    ///      }
    ///    },
    ///  }
    ///
    ///  Ok(())
    /// }
    /// ```
    pub async fn get_current_state(&self) -> Result<JobExecutionState> {
        Ok(
            crate::core::platform::job_execution::get_job_execution_by_id(
                self.job_name.as_str(),
                self.id,
            )
            .await?
            .state
            .into(),
        )
    }

    /// Get the state of the [`JobExecution`], one of [`JobExecutionState`].
    /// Note that this is the state at the time where the execution object was fetched.
    /// Use [`get_current_state`] to get the current state of the job execution from the hopsworks cluster.
    pub fn state(&self) -> JobExecutionState {
        self.state.clone()
    }

    /// Get the job name of the [`JobExecution`].
    pub fn job_name(&self) -> String {
        self.job_name.clone()
    }

    /// Get the submission time of the [`JobExecution`].
    pub fn submission_time(&self) -> String {
        self.submission_time.clone()
    }
}

/// Represents the state of a job execution.
///
/// The state of a job execution can be one of the following:
///
/// * Initializing
/// * Running
/// * Terminated
/// * Failed
///
/// The state of a job execution is represented by a string.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum JobExecutionState {
    Initializing,
    Running,
    Terminated,
    Failed,
}

impl From<String> for JobExecutionState {
    fn from(state: String) -> Self {
        match state.as_str() {
            "INITIALIZING" => Self::Initializing,
            "RUNNING" => Self::Running,
            "TERMINATED" => Self::Terminated,
            "FAILED" => Self::Failed,
            _ => panic!("Invalid job execution state: {}", state),
        }
    }
}
