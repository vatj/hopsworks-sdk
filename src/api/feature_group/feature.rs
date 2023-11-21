use serde::{Deserialize, Serialize};

use crate::repositories::feature::entities::FeatureDTO;

/// Feature entity gathering metadata about a feature in a feature group.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Feature {
    pub name: String,
    pub description: Option<String>,
    pub data_type: String,
    pub primary: bool,
    pub partition: bool,
    pub hudi_precombine_key: bool,
    pub feature_group_id: Option<i32>,
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
