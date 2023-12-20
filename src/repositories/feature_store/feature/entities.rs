use serde::{Deserialize, Serialize};

use crate::{
    feature_store::feature_group::feature::Feature,
    repositories::feature_store::{
        feature_group::entities::FeatureGroupDTO,
        transformation_function::entities::TransformationFunctionDTO,
    },
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FeatureDTO {
    pub name: String,
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub data_type: String,
    pub primary: bool,
    pub partition: bool,
    pub hudi_precombine_key: bool,
    pub feature_group_id: Option<i32>,
}

impl FeatureDTO {
    pub fn new(name: String, data_type: String) -> FeatureDTO {
        FeatureDTO {
            name,
            description: None,
            data_type,
            primary: false,
            partition: false,
            hudi_precombine_key: false,
            feature_group_id: None,
        }
    }
}

impl From<Feature> for FeatureDTO {
    fn from(feature: Feature) -> Self {
        FeatureDTO::new_from_feature(feature)
    }
}

impl FeatureDTO {
    pub fn new_from_feature(feature: Feature) -> Self {
        Self {
            feature_group_id: feature.get_feature_group_id(),
            primary: feature.is_primary(),
            name: feature.get_name(),
            description: feature.get_description(),
            data_type: feature.get_data_type(),
            partition: feature.defines_partition(),
            hudi_precombine_key: feature.is_hudi_precombine_key(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TrainingDatasetFeatureDTO {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub label: bool,
    pub feature_group_feature_name: String,
    pub transformation_function: Option<TransformationFunctionDTO>,
    pub index: i32,
    pub featuregroup: FeatureGroupDTO,
}

impl TrainingDatasetFeatureDTO {
    pub fn new_from_feature_and_transformation_function(
        feature: FeatureDTO,
        feature_group: FeatureGroupDTO,
        transformation_function: Option<TransformationFunctionDTO>,
    ) -> Self {
        Self {
            name: feature.name.clone(),
            data_type: feature.data_type,
            label: false,
            feature_group_feature_name: feature.name,
            transformation_function,
            index: 0,
            featuregroup: feature_group,
        }
    }
}
