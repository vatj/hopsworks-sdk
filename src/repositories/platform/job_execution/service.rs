use color_eyre::Result;
use reqwest::Method;

use super::JobExecutionDTO;
use crate::get_hopsworks_client;

pub async fn start_new_execution_for_named_job(job_name: &str) -> Result<JobExecutionDTO> {
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

pub async fn get_job_execution_by_id(
    job_name: &str,
    job_execution_id: i64,
) -> Result<JobExecutionDTO> {
    Ok(get_hopsworks_client()
        .await
        .request(
            Method::GET,
            format!("jobs/{job_name}/executions/{job_execution_id}").as_str(),
            true,
            true,
        )
        .await?
        .query(&[("sort_by", "submissiontime:desc")])
        .send()
        .await?
        .json::<JobExecutionDTO>()
        .await?)
}

pub async fn get_job_executions(job_name: &str) -> Result<Vec<JobExecutionDTO>> {
    Ok(get_hopsworks_client()
        .await
        .request(
            Method::GET,
            format!("jobs/{job_name}/executions").as_str(),
            true,
            true,
        )
        .await?
        .send()
        .await?
        .json::<Vec<JobExecutionDTO>>()
        .await?)
}

pub async fn delete_job_execution(job_name: &str, job_execution_id: i64) -> Result<()> {
    let response = get_hopsworks_client()
        .await
        .request(
            Method::DELETE,
            format!("jobs/{job_name}/executions/{job_execution_id}").as_str(),
            true,
            true,
        )
        .await?
        .send()
        .await?;

    if response.status().is_success() {
        Ok(())
    } else {
        Err(response.error_for_status().unwrap_err().into())
    }
}
