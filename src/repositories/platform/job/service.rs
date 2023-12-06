use color_eyre::Result;
use reqwest::Method;

use crate::get_hopsworks_client;

use super::JobDTO;

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
