use color_eyre::Result;

use crate::cluster_api::platform::job_execution::{self, JobExecutionDTO};

use super::file_system::download;

pub async fn download_job_execution_logs(
    job_name: &str,
    job_execution_id: i32,
    local_dir: Option<&str>,
) -> Result<()> {
    let job_execution_dto =
        job_execution::service::get_job_execution_by_id(job_name, job_execution_id).await?;

    download(
        job_execution_dto
            .stdout_path
            .expect("Job Execution stdout_path is not set.")
            .as_str(),
        local_dir,
        true,
    )
    .await?;

    download(
        job_execution_dto
            .stderr_path
            .expect("Job Execution stderr_path is not set.")
            .as_str(),
        local_dir,
        true,
    )
    .await?;

    Ok(())
}

pub async fn start_new_execution_for_named_job(
    job_name: &str,
    job_args: Option<&str>,
) -> Result<JobExecutionDTO> {
    job_execution::service::start_new_execution_for_named_job(job_name, job_args.unwrap_or(""))
        .await
}

pub async fn stop_job_execution(job_name: &str, job_execution_id: i32) -> Result<JobExecutionDTO> {
    job_execution::service::stop_job_execution(job_name, job_execution_id).await
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
