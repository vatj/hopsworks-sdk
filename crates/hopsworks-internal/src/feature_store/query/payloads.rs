use serde::{Deserialize, Serialize};

use crate::feature_store::{
        feature::FeatureDTO, feature_group::FeatureGroupDTO,
    };

use super::{QueryFilterOrLogicDTO, enums::JoinType};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewQueryPayload {
    feature_store_name: String,
    feature_store_id: i32,
    left_feature_group: FeatureGroupDTO,
    left_features: Vec<FeatureDTO>,
    left_feature_group_start_time: Option<String>,
    left_feature_group_end_time: Option<String>,
    joins: Option<Vec<NewJoinQueryPayload>>,
    hive_engine: bool,
    filters: Option<Vec<QueryFilterOrLogicDTO>>,
}

impl NewQueryPayload {
    pub fn new(
        left_feature_group: FeatureGroupDTO,
        left_features: Vec<FeatureDTO>,
        left_feature_group_start_time: Option<String>,
        left_feature_group_end_time: Option<String>,
        joins: Option<Vec<NewJoinQueryPayload>>,
        hive_engine: bool,
        filters: Option<Vec<QueryFilterOrLogicDTO>>,
    ) -> Self {
        Self {
            feature_store_name: left_feature_group.featurestore_name.clone(),
            feature_store_id: left_feature_group.featurestore_id,
            left_feature_group,
            left_features,
            left_feature_group_start_time,
            left_feature_group_end_time,
            joins,
            hive_engine,
            filters,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewJoinQueryPayload {
    query: NewQueryPayload,
    on: Option<Vec<String>>,
    left_on: Option<Vec<String>>,
    right_on: Option<Vec<String>>,
    #[serde(rename = "type")]
    join_type: JoinType,
    prefix: Option<String>,
}

impl NewJoinQueryPayload {
    pub fn new(query: NewQueryPayload, join_config: PayloadJoinConfig) -> Self {
        Self {
            query,
            on: join_config.on,
            left_on: join_config.left_on,
            right_on: join_config.right_on,
            join_type: join_config.join_type,
            prefix: join_config.prefix,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PayloadJoinConfig {
    pub on: Option<Vec<String>>,
    pub left_on: Option<Vec<String>>,
    pub right_on: Option<Vec<String>>,
    pub join_type: JoinType,
    pub prefix: Option<String>,
}

impl PayloadJoinConfig {
    pub fn new(
        on: Option<Vec<String>>,
        left_on: Option<Vec<String>>,
        right_on: Option<Vec<String>>,
        join_type: JoinType,
        prefix: Option<String>,
    ) -> Self {
        Self {
            on,
            left_on,
            right_on,
            join_type,
            prefix,
        }
    }
}
