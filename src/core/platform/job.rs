use color_eyre::Result;

use crate::repositories::platform::job::{
    self,
    entities::{JobDTO, JobExecutionDTO},
};

pub async fn get_job_by_name(job_name: &str) -> Result<JobDTO> {
    job::service::get_job_by_name(job_name).await
}

pub async fn run_job_with_name(job_name: &str) -> Result<JobExecutionDTO> {
    job::service::run_job_with_name(job_name).await
}
