use color_eyre::Result;
use reqwest::Method;

use crate::get_hopsworks_client;

use super::JobDTO;

pub async fn get_job_by_name(job_name: &str) -> Result<JobDTO> {
    Ok(get_hopsworks_client()
        .await
        .request(Method::GET, format!("jobs/{job_name}").as_str(), true, true)
        .await?
        .query(&[("expand", ["creator"])])
        .send()
        .await?
        .json::<JobDTO>()
        .await?)
}

pub async fn get_jobs() -> Result<Vec<JobDTO>> {
    Ok(get_hopsworks_client()
        .await
        .request(Method::GET, "jobs", true, true)
        .await?
        .query(&[("expand", ["creator"])])
        .send()
        .await?
        .json::<Vec<JobDTO>>()
        .await?)
}

pub async fn exists(job_name: &str) -> Result<bool> {
    let response = get_job_by_name(job_name).await;
    match response {
        Ok(_) => Ok(true),
        Err(e) => {
            if e.to_string().contains("404") {
                Ok(false)
            } else {
                Err(e)
            }
        }
    }
}

pub async fn delete_job(job_name: &str) -> Result<()> {
    let response = get_hopsworks_client()
        .await
        .request(
            Method::DELETE,
            format!("jobs/{job_name}").as_str(),
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

pub async fn get_job_configuration(job_type: &str) -> Result<serde_json::Value> {
    Ok(get_hopsworks_client()
        .await
        .request(
            Method::GET,
            format!("jobs/{job_type}/configuration").as_str(),
            true,
            true,
        )
        .await?
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?)
}

pub async fn create_job(job_name: &str, job_config: serde_json::Value) -> Result<JobDTO> {
    Ok(get_hopsworks_client()
        .await
        .request(
            Method::POST,
            format!("jobs/{job_name}").as_str(),
            true,
            true,
        )
        .await?
        .json(&job_config)
        .send()
        .await?
        .json::<JobDTO>()
        .await?)
}

pub async fn update_job(job_name: &str, job_config: serde_json::Value) -> Result<JobDTO> {
    Ok(get_hopsworks_client()
        .await
        .request(Method::PUT, format!("jobs/{job_name}").as_str(), true, true)
        .await?
        .json(&job_config)
        .send()
        .await?
        .json::<JobDTO>()
        .await?)
}
