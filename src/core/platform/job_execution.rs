use color_eyre::Result;

use crate::repositories::platform::job_execution::{self, JobExecutionDTO};

pub async fn download_job_execution_logs() -> Result<()> {
    todo!()
}

pub async fn start_new_execution_for_named_job(job_name: &str) -> Result<JobExecutionDTO> {
    job_execution::service::start_new_execution_for_named_job(job_name).await
}

pub async fn get_job_execution_by_id(
    job_name: &str,
    job_execution_id: i32,
) -> Result<JobExecutionDTO> {
    job_execution::service::get_job_execution_by_id(job_name, job_execution_id).await
}

pub async fn get_job_executions(job_name: &str) -> Result<Vec<JobExecutionDTO>> {
    job_execution::service::get_job_executions(job_name).await
}

pub async fn delete_job_execution(job_name: &str, job_execution_id: i32) -> Result<()> {
    job_execution::service::delete_job_execution(job_name, job_execution_id).await
}

pub async fn await_termination(job_name: &str, job_execution_id: i32) -> Result<()> {
    while {
        let job_execution = get_job_execution_by_id(job_name, job_execution_id).await?;
        job_execution.state == "TERMINATED"
    } {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
    Ok(())
}
