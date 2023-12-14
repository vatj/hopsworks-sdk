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
    pub(crate) href: String,
    pub(crate) id: i32,
    submission_time: String,
    pub(crate) state: String,
    hdfs_user: String,
    args: String,
    final_status: String,
    progress: f64,
    user: JobExecutionUserDTO,
    pub(crate) files_to_remove: Vec<String>,
    duration: i64,
    pub(crate) job: Option<JobDTO>,
    pub(crate) stdout_path: Option<String>,
    pub(crate) stderr_path: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JobExecutionUserDTO {
    href: String,
}
