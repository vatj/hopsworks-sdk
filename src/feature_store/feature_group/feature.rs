use serde::{Deserialize, Serialize};

use crate::repositories::feature_store::feature::entities::FeatureDTO;

/// Feature entity gathering metadata about a feature in a feature group.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Feature {
    name: String,
    description: Option<String>,
    data_type: String,
    primary: bool,
    partition: bool,
    hudi_precombine_key: bool,
    feature_group_id: Option<i32>,
}

impl Feature {
    pub fn new_from_dto(feature_dto: FeatureDTO) -> Self {
        Self {
            name: feature_dto.name,
            description: feature_dto.description,
            data_type: feature_dto.data_type,
            primary: feature_dto.primary,
            partition: feature_dto.partition,
            hudi_precombine_key: feature_dto.hudi_precombine_key,
            feature_group_id: feature_dto.feature_group_id,
        }
    }
}

impl From<FeatureDTO> for Feature {
    fn from(feature_dto: FeatureDTO) -> Self {
        Feature::new_from_dto(feature_dto)
    }
}

impl Feature {
    pub fn get_feature_group_id(&self) -> Option<i32> {
        self.feature_group_id
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_description(&self) -> Option<String> {
        self.description.clone()
    }

    pub fn is_primary(&self) -> bool {
        self.primary
    }

    pub fn is_hudi_precombine_key(&self) -> bool {
        self.hudi_precombine_key
    }

    pub fn defines_partition(&self) -> bool {
        self.partition
    }

    pub fn get_data_type(&self) -> String {
        self.data_type.clone()
    }
}
