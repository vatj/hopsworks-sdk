use serde::{Deserialize, Serialize};

use crate::{
    api::query::entities::{JoinQuery, Query},
    repositories::{feature::entities::FeatureDTO, feature_group::entities::FeatureGroupDTO, storage_connector::payloads::ExternalFeatureGroupConnectorArrowFlightPayload},
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewQueryPayload<'a> {
    feature_store_name: String,
    feature_store_id: i32,
    left_feature_group: FeatureGroupDTO,
    left_features: Vec<FeatureDTO>,
    left_feature_group_start_time: Option<&'a str>,
    left_feature_group_end_time: Option<&'a str>,
    joins: Option<Vec<NewJoinQueryPayload<'a>>>,
    hive_engine: bool,
    filter: Option<&'a str>,
}

impl<'a> NewQueryPayload<'a> {
    pub fn new(
        left_feature_group: FeatureGroupDTO,
        left_features: Vec<FeatureDTO>,
        left_feature_group_start_time: Option<&'a str>,
        left_feature_group_end_time: Option<&'a str>,
        joins: Option<Vec<NewJoinQueryPayload<'a>>>,
        hive_engine: bool,
        filter: Option<&'a str>,
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
            filter,
        }
    }
}

impl<'a> From<Query> for NewQueryPayload<'a> {
    fn from(query: Query) -> Self {
        Self {
            feature_store_name: query.feature_store_name,
            feature_store_id: query.feature_store_id,
            left_feature_group: FeatureGroupDTO::from(query.left_feature_group),
            left_features: query
                .left_features
                .iter()
                .map(|feature| FeatureDTO::from(feature.clone()))
                .collect(),
            left_feature_group_start_time: None,
            left_feature_group_end_time: None,
            joins: query.joins.map(|joins| {
                joins
                    .iter()
                    .map(|join_query| NewJoinQueryPayload::from(join_query.clone()))
                    .collect()
            }),
            hive_engine: true,
            filter: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewJoinQueryPayload<'a> {
    feature_store_name: String,
    feature_store_id: i32,
    left_feature_group: FeatureGroupDTO,
    left_features: Vec<FeatureDTO>,
    left_feature_group_start_time: Option<&'a str>,
    left_feature_group_end_time: Option<&'a str>,
    joins: Vec<NewJoinQueryPayload<'a>>,
    hive_engine: bool,
    filter: Option<String>,
    on: Vec<String>,
    left_on: Vec<String>,
    right_on: Vec<String>,
    #[serde(rename = "type")]
    join_type: String,
}

impl<'a> NewJoinQueryPayload<'a> {
    pub fn new(
        left_feature_group: FeatureGroupDTO,
        left_features: Vec<FeatureDTO>,
        left_feature_group_start_time: Option<&'a str>,
        left_feature_group_end_time: Option<&'a str>,
        joins: Vec<NewJoinQueryPayload<'a>>,
        hive_engine: bool,
        join_config: PayloadJoinConfig,
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
            filter: join_config.filter,
            on: join_config.on,
            left_on: join_config.left_on,
            right_on: join_config.right_on,
            join_type: join_config.join_type,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PayloadJoinConfig {
    pub filter: Option<String>,
    pub on: Vec<String>,
    pub left_on: Vec<String>,
    pub right_on: Vec<String>,
    pub join_type: String,
}

impl PayloadJoinConfig {
    pub fn new(
        filter: Option<String>,
        on: Vec<String>,
        left_on: Vec<String>,
        right_on: Vec<String>,
        join_type: String,
    ) -> Self {
        Self {
            filter,
            on,
            left_on,
            right_on,
            join_type,
        }
    }
}

impl<'a> From<JoinQuery> for NewJoinQueryPayload<'a> {
    fn from(join_query: JoinQuery) -> Self {
        Self {
            feature_store_name: join_query.feature_store_name,
            feature_store_id: join_query.feature_store_id,
            left_feature_group: FeatureGroupDTO::from(join_query.left_feature_group),
            left_features: join_query
                .left_features
                .iter()
                .map(|feature| FeatureDTO::from(feature.clone()))
                .collect(),
            left_feature_group_start_time: None,
            left_feature_group_end_time: None,
            joins: vec![],
            hive_engine: false,
            filter: None,
            on: join_query.on,
            left_on: join_query.left_on,
            right_on: join_query.right_on,
            join_type: join_query.join_type,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryArrowFlightPayload {
    pub query_string: String,
    pub connectors: Option<Vec<ExternalFeatureGroupConnectorArrowFlightPayload>>,
    pub filters: Option<Vec<FilterArrowFlightPayload>>,
    pub features: Vec<String>
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FilterArrowFlightPayload {
    #[serde(rename = "type")]
    pub filter_type: String,
    pub logic_type: Option<String>,
    pub left_filter: Option<Vec<FilterArrowFlightPayload>>,
    pub right_filter: Option<Vec<FilterArrowFlightPayload>>,
    pub condition: Option<String>,
    pub value: Option<String>,
    pub feature: Option<String>
}
