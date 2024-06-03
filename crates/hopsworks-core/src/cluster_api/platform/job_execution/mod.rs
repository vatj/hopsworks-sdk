pub mod service;

use serde::{Deserialize, Serialize};

use super::job::JobDTO;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JobExectutionMinimalDTO {
    href: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JobExecutionDTO {
    pub href: String,
    pub id: i32,
    pub submission_time: String,
    pub state: String,
    hdfs_user: String,
    args: String,
    final_status: String,
    progress: f64,
    user: JobExecutionUserDTO,
    pub files_to_remove: Vec<String>,
    duration: i64,
    pub job: Option<JobDTO>,
    pub stdout_path: Option<String>,
    pub stderr_path: Option<String>,
    pub job_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JobExecutionUserDTO {
    href: String,
}
