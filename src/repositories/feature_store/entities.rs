use serde::{Deserialize, Serialize};

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
    pub hdfs_store_path: String,
    pub project_name: String,
    pub project_id: i32,
    pub featurestore_description: String,
    pub inode_id: i32,
    pub online_featurestore_name: String,
    pub online_featurestore_size: f64,
    pub hive_endpoint: String,
    pub mysql_server_endpoint: String,
    pub online_enabled: bool,
}
