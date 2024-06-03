use serde::{Deserialize, Serialize};

pub(crate) mod service;

pub(crate) mod feature;
pub(crate) mod feature_group;
pub(crate) mod feature_view;
pub(crate) mod query;
pub(crate) mod statistics_config;
pub(crate) mod storage_connector;
pub(crate) mod training_dataset;
pub(crate) mod transformation_function;
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FeatureStoreDTO {
    pub(crate) num_feature_groups: i32,
    pub(crate) num_training_datasets: i32,
    pub(crate) num_storage_connectors: i32,
    pub(crate) num_feature_views: i32,
    pub(crate) featurestore_id: i32,
    pub(crate) featurestore_name: String,
    pub(crate) created: String,
    pub(crate) hdfs_store_path: Option<String>,
    pub(crate) project_name: String,
    pub(crate) project_id: i32,
    pub(crate) featurestore_description: Option<String>,
    pub(crate) inode_id: Option<i32>,
    pub(crate) online_featurestore_name: String,
    pub(crate) online_featurestore_size: Option<f64>,
    pub(crate) hive_endpoint: String,
    pub(crate) mysql_server_endpoint: String,
    pub(crate) online_enabled: bool,
}
