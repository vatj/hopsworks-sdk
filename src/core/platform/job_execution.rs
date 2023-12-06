use color_eyre::Result;

use crate::repositories::platform::job_execution::{self, JobExecutionDTO};

pub async fn download_logs() -> Result<()> {
    todo!()
}

pub async fn delete() -> Result<()> {
    todo!()
}

pub async fn start_new_execution_for_named_job(job_name: &str) -> Result<JobExecutionDTO> {
    job_execution::service::start_new_execution_for_named_job(job_name).await
}
