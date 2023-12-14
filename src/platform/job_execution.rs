use color_eyre::Result;
use serde::{Deserialize, Serialize};

use crate::repositories::platform::job_execution::JobExecutionDTO;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobExecutionUserDTO {
    href: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Execution {
    href: String,
    id: i32,
    job_name: String,
    state: JobExecutionState,
}

impl Execution {
    fn new_from_dto(execution_dto: JobExecutionDTO) -> Self {
        Self {
            href: execution_dto.href,
            id: execution_dto.id,
            job_name: execution_dto.job.expect("Job not provided").name,
            state: execution_dto.state.into(),
        }
    }
}

impl From<JobExecutionDTO> for Execution {
    fn from(execution_dto: JobExecutionDTO) -> Self {
        Self::new_from_dto(execution_dto)
    }
}

impl Execution {
    pub async fn download_logs(&self, local_dir: Option<&str>) -> Result<()> {
        crate::core::platform::job_execution::download_job_execution_logs(
            self.job_name.as_str(),
            self.id,
            local_dir,
        )
        .await
    }

    pub async fn delete(&self) -> Result<()> {
        crate::core::platform::job_execution::delete_job_execution(self.job_name.as_str(), self.id)
            .await
    }

    pub async fn await_termination(&self) -> Result<()> {
        crate::core::platform::job_execution::await_termination(self.job_name.as_str(), self.id)
            .await
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
#[derive(Debug, Serialize, Deserialize, Clone)]
enum JobExecutionState {
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
