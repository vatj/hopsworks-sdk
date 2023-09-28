use color_eyre::Result;
use reqwest::Method;

use crate::get_hopsworks_client;

use super::entities::{JobDTO, JobExecutionDTO};

pub async fn get_job_by_name(job_name: &str) -> Result<JobDTO> {
    Ok(get_hopsworks_client()
        .await
        .request(Method::GET, format!("jobs/{job_name}").as_str(), true, true)
        .await?
        .send()
        .await?
        .json::<JobDTO>()
        .await?)
}

pub async fn run_job_with_name(job_name: &str) -> Result<JobExecutionDTO> {
    Ok(get_hopsworks_client()
        .await
        .request(Method::POST, format!("jobs/{job_name}/executions").as_str(), true, true)
        .await?
        .send()
        .await?
        .json::<JobExecutionDTO>()
        .await?)
}
