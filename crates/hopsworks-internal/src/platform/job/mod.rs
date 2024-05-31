use serde::{Deserialize, Serialize};

use crate::platform::job_execution::JobExectutionMinimalDTO;

pub mod service;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JobDTO {
    pub(crate) href: String,
    pub(crate) id: i32,
    pub(crate) name: String,
    pub(crate) creation_time: String,
    pub(crate) config: serde_json::Value,
    pub(crate) job_type: String,
    creator: JobCreatorDTO,
    executions: JobExectutionMinimalDTO,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JobCreatorDTO {
    href: String,
    firstname: Option<String>,
    lastname: Option<String>,
    email: Option<String>,
    username: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct JobConfigDTO {
    #[serde(rename = "type")]
    config_type: String,
    app_name: String,
    default_args: String,
    am_queue: String,
    am_memory: i32,
    am_v_cores: i32,
    app_path: String,
    main_class: String,
    job_type: String,
    #[serde(rename = "spark.executor.cores")]
    spark_executor_cores: i32,
    #[serde(rename = "spark.executor.memory")]
    spark_executor_memory: i32,
    #[serde(rename = "spark.executor.instances")]
    spark_executor_instances: i32,
    #[serde(rename = "spark.executor.gpus")]
    spark_executor_gpus: i32,
    #[serde(rename = "spark.dynamicAllocation.enabled")]
    spark_dynamic_allocation_enabled: bool,
    #[serde(rename = "spark.dynamicAllocation.minExecutors")]
    spark_dynamic_allocation_min_executors: i32,
    #[serde(rename = "spark.dynamicAllocation.maxExecutors")]
    spark_dynamic_allocation_max_executors: i32,
    #[serde(rename = "spark.dynamicAllocation.initialExecutors")]
    spark_dynamic_allocation_initial_executors: i32,
    #[serde(rename = "spark.blacklist.enabled")]
    spark_blacklist_enabled: bool,
    #[serde(rename = "spark.tensorflow.num.ps")]
    spark_tensorflow_num_ps: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JobListDTO {
    href: String,
    items: Vec<JobDTO>,
}
