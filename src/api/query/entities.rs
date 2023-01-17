use serde::{Deserialize, Serialize};

use crate::api::feature_group::entities::{Feature, FeatureGroup};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Query {
    pub left_feature_group: FeatureGroup,
    pub left_features: Vec<Feature>,
    pub feature_store_name: String,
    pub feature_store_id: i32,
    pub joins: Vec<JoinQuery>,
}

impl Query {
    pub fn new(left_feature_group: FeatureGroup, left_features: Vec<Feature>) -> Self {
        Self {
            feature_store_name: left_feature_group.featurestore_name.clone(),
            feature_store_id: left_feature_group.featurestore_id,
            left_feature_group,
            left_features,
            joins: vec![],
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
