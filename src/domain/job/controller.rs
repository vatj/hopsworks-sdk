use color_eyre::Result;

use crate::repositories::{
    self,
    job::entities::{JobDTO, JobExecutionDTO},
};

pub async fn get_job_by_name_and_project_id(project_id: i32, job_name: &str) -> Result<JobDTO> {
    repositories::job::service::get_job_by_name_and_project_id(project_id, job_name).await
}

pub async fn run_job_with_name(project_id: i32, job_name: &str) -> Result<JobExecutionDTO> {
    repositories::job::service::run_job_with_name(project_id, job_name).await
}
