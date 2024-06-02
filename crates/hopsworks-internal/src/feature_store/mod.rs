use serde::{Deserialize, Serialize};

pub mod service;

pub mod feature;
pub mod feature_group;
pub mod feature_view;
pub mod query;
pub mod statistics_config;
pub mod storage_connector;
pub mod training_dataset;
pub mod transformation_function;
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FeatureStoreDTO {
    pub num_feature_groups: i32,
    pub num_training_datasets: i32,
    pub num_storage_connectors: i32,
    pub num_feature_views: i32,
    pub featurestore_id: i32,
    pub featurestore_name: String,
    pub created: String,
    pub hdfs_store_path: Option<String>,
    pub project_name: String,
    pub project_id: i32,
    pub featurestore_description: Option<String>,
    pub inode_id: Option<i32>,
    pub online_featurestore_name: String,
    pub online_featurestore_size: Option<f64>,
    pub hive_endpoint: String,
    pub mysql_server_endpoint: String,
    pub online_enabled: bool,
}
