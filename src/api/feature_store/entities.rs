use serde::{Deserialize, Serialize};

use crate::repositories::feature_store::entities::FeatureStoreDTO;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FeatureStore {
    num_feature_groups: i32,
    num_training_datasets: i32,
    num_storage_connectors: i32,
    num_feature_views: i32,
    pub featurestore_id: i32,
    pub featurestore_name: String,
    created: String,
    project_name: String,
    project_id: i32,
    featurestore_description: String,
    online_featurestore_name: String,
    online_featurestore_size: f64,
    online_enabled: bool,
}

impl FeatureStore {
    pub fn new_from_dto(feature_store_dto: FeatureStoreDTO) -> Self {
        Self {
            num_feature_groups: feature_store_dto.num_feature_groups,
            num_training_datasets: feature_store_dto.num_training_datasets,
            num_storage_connectors: feature_store_dto.num_storage_connectors,
            num_feature_views: feature_store_dto.num_feature_views,
            featurestore_id: feature_store_dto.featurestore_id,
            featurestore_name: feature_store_dto.featurestore_name,
            created: feature_store_dto.created,
            project_name: feature_store_dto.project_name,
            project_id: feature_store_dto.project_id,
            featurestore_description: feature_store_dto.featurestore_description,
            online_featurestore_name: feature_store_dto.online_featurestore_name,
            online_featurestore_size: feature_store_dto.online_featurestore_size,
            online_enabled: feature_store_dto.online_enabled,
        }
    }
}
