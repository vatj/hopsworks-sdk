use serde::{Deserialize, Serialize};

use crate::{
    api::feature_group::entities::{Feature, FeatureGroup},
    repositories::query::entities::QueryDTO,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Query {
    pub left_feature_group: FeatureGroup,
    pub left_features: Vec<Feature>,
    pub feature_store_name: String,
    pub feature_store_id: i32,
    pub joins: Option<Vec<JoinQuery>>,
}

impl Query {
    pub fn new(left_feature_group: FeatureGroup, left_features: Vec<Feature>) -> Self {
        Self {
            feature_store_name: left_feature_group.featurestore_name.clone(),
            feature_store_id: left_feature_group.featurestore_id,
            left_feature_group,
            left_features,
            joins: Some(vec![]),
        }
    }
}

impl From<QueryDTO> for Query {
    fn from(dto: QueryDTO) -> Self {
        Self {
            left_feature_group: FeatureGroup::from(dto.left_feature_group),
            left_features: dto
                .left_features
                .iter()
                .map(|feature_dto| Feature::from(feature_dto.clone()))
                .collect(),
            feature_store_name: dto.feature_store_name.clone(),
            feature_store_id: dto.feature_store_id,
            joins: Some(vec![]),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JoinQuery {
    pub left_feature_group: FeatureGroup,
    pub left_features: Vec<Feature>,
    pub feature_store_name: String,
    pub feature_store_id: i32,
    pub joins: Vec<JoinQuery>,
    pub filter: Option<String>,
    pub on: Vec<String>,
    pub left_on: Vec<String>,
    pub right_on: Vec<String>,
    pub join_type: String,
}

impl JoinQuery {
    pub fn new(left_feature_group: FeatureGroup, left_features: Vec<Feature>) -> Self {
        Self {
            feature_store_id: left_feature_group.featurestore_id,
            feature_store_name: left_feature_group.featurestore_name.clone(),
            left_feature_group,
            left_features,
            joins: vec![],
            filter: None,
            on: vec![],
            left_on: vec![],
            right_on: vec![],
            join_type: String::from("INNER"),
        }
    }
}
