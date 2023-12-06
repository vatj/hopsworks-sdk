use color_eyre::Result;
use reqwest::Method;

use super::JobExecutionDTO;
use crate::get_hopsworks_client;

pub async fn start_new_execution_for_named_job(job_name: &str) -> Result<JobExecutionDTO> {
    // Starts new execution for a job
    Ok(get_hopsworks_client()
        .await
        .request(
            Method::POST,
            format!("jobs/{job_name}/executions").as_str(),
            true,
            true,
        )
        .await?
        .send()
        .await?
        .json::<JobExecutionDTO>()
        .await?)
}

pub async fn delete_job_execution() -> Result<()> {
    todo!()
}
