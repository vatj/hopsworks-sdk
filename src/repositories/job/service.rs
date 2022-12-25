use color_eyre::Result;

use crate::get_hopsworks_client;

use super::entities::{JobDTO, JobExecutionDTO};

pub async fn get_job_by_name(job_name: &str) -> Result<JobDTO> {
    Ok(get_hopsworks_client()
        .await
        .send_get(format!("jobs/{job_name}").as_str(), true)
        .await?
        .json::<JobDTO>()
        .await?)
}

pub async fn run_job_with_name(job_name: &str) -> Result<JobExecutionDTO> {
    Ok(get_hopsworks_client()
        .await
        .send_empty_post(format!("jobs/{job_name}/executions").as_str(), true)
        .await?
        .json::<JobExecutionDTO>()
        .await?)
}
