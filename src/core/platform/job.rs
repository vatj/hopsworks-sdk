use color_eyre::Result;

use crate::repositories::platform::{job, job_execution};

pub async fn get_job_by_name(job_name: &str) -> Result<job::JobDTO> {
    job::service::get_job_by_name(job_name).await
}

pub async fn run_job_with_name(job_name: &str) -> Result<job_execution::JobExecutionDTO> {
    crate::core::platform::job_execution::start_new_execution_for_named_job(job_name).await
}
