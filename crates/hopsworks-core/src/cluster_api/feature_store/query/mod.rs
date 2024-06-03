use serde::{Deserialize, Serialize};
use std::fmt;

use crate::cluster_api::feature_store::{feature::FeatureDTO, feature_group::FeatureGroupDTO};

pub mod payloads;
pub mod service;

use crate::feature_store::query::enums::{JoinType, QueryFilterCondition, QueryLogicType};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryDTO {
    pub href: Option<String>,
    pub left_feature_group: FeatureGroupDTO,
    pub left_features: Vec<FeatureDTO>,
    pub feature_store_name: String,
    pub feature_store_id: i32,
    pub joins: Option<Vec<JoinQueryDTO>>,
    pub filters: Option<Vec<QueryFilterOrLogicDTO>>,
    pub left_feature_group_start_time: Option<String>,
    pub left_feature_group_end_time: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FeatureStoreQueryDTO {
    href: Option<String>,
    pub query: String,
    pub query_online: String,
    pub pit_query: Option<String>,
    pub pit_query_asof: Option<String>,
    pub hudi_cached_feature_groups: Vec<FeatureGroupDTO>,
    pub on_demand_feature_groups: Vec<FeatureGroupDTO>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JoinQueryDTO {
    pub query: QueryDTO,
    #[serde(rename = "type")]
    pub join_type: JoinType,
    pub on: Option<Vec<String>>,
    pub left_on: Option<Vec<String>>,
    pub right_on: Option<Vec<String>>,
    pub prefix: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryFilterDTO {
    pub feature: FeatureDTO,
    pub condition: QueryFilterCondition,
    pub value: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryLogicDTO {
    pub logic_type: QueryLogicType,
    pub left_logic: Option<Box<QueryLogicDTO>>,
    pub right_logic: Option<Box<QueryLogicDTO>>,
    pub left_filter: Option<QueryFilterDTO>,
    pub right_filter: Option<QueryFilterDTO>,
}

pub enum QueryFilterOrLogicDTO {
    Logic(QueryLogicDTO),
    Filter(QueryFilterDTO),
}

impl Serialize for QueryFilterOrLogicDTO {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            QueryFilterOrLogicDTO::Logic(logic) => logic.serialize(serializer),
            QueryFilterOrLogicDTO::Filter(filter) => filter.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for QueryFilterOrLogicDTO {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value.get("type").is_some() {
            Ok(QueryFilterOrLogicDTO::Logic(
                QueryLogicDTO::deserialize(value).unwrap(),
            ))
        } else {
            Ok(QueryFilterOrLogicDTO::Filter(
                QueryFilterDTO::deserialize(value).unwrap(),
            ))
        }
    }
}

impl fmt::Debug for QueryFilterOrLogicDTO {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryFilterOrLogicDTO::Logic(logic) => logic.fmt(f),
            QueryFilterOrLogicDTO::Filter(filter) => filter.fmt(f),
        }
    }
}

impl Clone for QueryFilterOrLogicDTO {
    fn clone(&self) -> Self {
        match self {
            QueryFilterOrLogicDTO::Logic(logic) => QueryFilterOrLogicDTO::Logic(logic.clone()),
            QueryFilterOrLogicDTO::Filter(filter) => QueryFilterOrLogicDTO::Filter(filter.clone()),
        }
    }
}
