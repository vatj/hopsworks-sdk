use color_eyre::Result;
use reqwest::Method;

use super::JobExecutionDTO;
use hopsworks_base::get_hopsworks_client;

pub async fn start_new_execution_for_named_job(
    job_name: &str,
    args: &str,
) -> Result<JobExecutionDTO> {
    let response = get_hopsworks_client()
        .await
        .request(
            Method::POST,
            format!("jobs/{job_name}/executions").as_str(),
            true,
            true,
        )
        .await?
        .header("Content-Type", "text/plain")
        .body(args.to_string())
        .send()
        .await?;

    if response.status().is_success() {
        let mut job_execution_dto = response.json::<JobExecutionDTO>().await?;
        job_execution_dto.job_name = Some(job_name.to_string());
        Ok(job_execution_dto)
    } else {
        Err(response.error_for_status().unwrap_err().into())
    }
}

pub async fn get_job_execution_by_id(
    job_name: &str,
    job_execution_id: i32,
) -> Result<JobExecutionDTO> {
    let response = get_hopsworks_client()
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
        .await?;

    if response.status().is_success() {
        let mut job_execution_dto = response.json::<JobExecutionDTO>().await?;
        job_execution_dto.job_name = Some(job_name.to_string());
        Ok(job_execution_dto)
    } else {
        Err(response.error_for_status().unwrap_err().into())
    }
}

pub async fn get_job_executions(job_name: &str) -> Result<Vec<JobExecutionDTO>> {
    let mut job_execution_dtos = get_hopsworks_client()
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
        .await?;

    for job_execution_dto in job_execution_dtos.iter_mut() {
        job_execution_dto.job_name = Some(job_name.to_string());
    }
    Ok(job_execution_dtos)
}

pub async fn stop_job_execution(job_name: &str, job_execution_id: i32) -> Result<JobExecutionDTO> {
    let response = get_hopsworks_client()
        .await
        .request(
            Method::PUT,
            format!("jobs/{job_name}/executions/{job_execution_id}").as_str(),
            true,
            true,
        )
        .await?
        .json(&serde_json::json!({"state": "stopped"}))
        .send()
        .await?;

    if response.status().is_success() {
        let mut job_execution_dto = response.json::<JobExecutionDTO>().await?;
        job_execution_dto.job_name = Some(job_name.to_string());
        Ok(job_execution_dto)
    } else {
        Err(response.error_for_status().unwrap_err().into())
    }
}

pub async fn delete_job_execution(job_name: &str, job_execution_id: i32) -> Result<()> {
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
