use color_eyre::Result;

use crate::get_hopsworks_client;

use super::entities::{JobDTO, JobExecutionDTO};

pub async fn get_job_by_name_and_project_id(project_id: i32, job_name: &str) -> Result<JobDTO> {
    Ok(get_hopsworks_client()
        .await
        .send_get(format!("project/{project_id}/jobs/{job_name}").as_str())
        .await?
        .json::<JobDTO>()
        .await?)
}

pub async fn run_job_with_name(project_id: i32, job_name: &str) -> Result<JobExecutionDTO> {
    Ok(get_hopsworks_client()
        .await
        .send_empty_post(format!("project/{project_id}/jobs/{job_name}/executions").as_str())
        .await?
        .json::<JobExecutionDTO>()
        .await?)
}
