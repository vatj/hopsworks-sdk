use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FeatureStoreDTO {
    num_feature_groups: i32,
    num_training_datasets: i32,
    num_storage_connectors: i32,
    num_feature_views: i32,
    featurestore_id: i32,
    featurestore_name: String,
    created: String,
    hdfs_store_path: String,
    project_name: String,
    project_id: i32,
    featurestore_description: String,
    inode_id: i32,
    online_featurestore_name: String,
    online_featurestore_size: f64,
    hive_endpoint: String,
    mysql_server_endpoint: String,
    online_enabled: bool,
}
