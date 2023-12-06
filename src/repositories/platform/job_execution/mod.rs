pub mod service;

use serde::{Deserialize, Serialize};

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
    state: String,
    hdfs_user: String,
    args: String,
    final_status: String,
    progress: f64,
    user: JobExecutionUserDTO,
    files_to_remove: Vec<String>,
    duration: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JobExecutionUserDTO {
    href: String,
}
