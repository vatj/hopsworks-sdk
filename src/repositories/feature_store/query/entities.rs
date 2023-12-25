use serde::{Deserialize, Serialize};
use std::fmt;

use crate::{
    feature_store::{
        feature_group::feature::Feature,
        query::{
            filter::{QueryFilterCondition, QueryLogicType},
            join::JoinType,
            JoinQuery, Query, QueryFilter, QueryFilterOrLogic, QueryLogic,
        },
    },
    repositories::feature_store::{
        feature::entities::FeatureDTO, feature_group::entities::FeatureGroupDTO,
    },
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryDTO {
    href: Option<String>,
    pub left_feature_group: FeatureGroupDTO,
    pub left_features: Vec<FeatureDTO>,
    pub feature_store_name: String,
    pub feature_store_id: i32,
    pub joins: Option<Vec<JoinQueryDTO>>,
    pub filters: Option<Vec<QueryFilterOrLogicDTO>>,
    pub left_feature_group_start_time: Option<String>,
    pub left_feature_group_end_time: Option<String>,
}

impl From<Query> for QueryDTO {
    fn from(query: Query) -> Self {
        Self {
            href: None,
            feature_store_name: String::from(query.feature_store_name()),
            feature_store_id: query.feature_store_id(),
            left_feature_group: FeatureGroupDTO::from(query.left_feature_group().clone()),
            left_features: query
                .left_features()
                .iter()
                .map(|feature| FeatureDTO::from(feature.clone()))
                .collect(),
            joins: match query.joins() {
                Some(joins) => Some(
                    joins
                        .iter()
                        .map(|join| JoinQueryDTO::from(join.clone()))
                        .collect(),
                ),
                None => None,
            },
            filters: match query.filters() {
                Some(filters) => Some(
                    filters
                        .iter()
                        .map(|filter| QueryFilterOrLogicDTO::from(filter.clone()))
                        .collect(),
                ),
                None => None,
            },
            left_feature_group_start_time: query
                .left_feature_group_start_time()
                .map(str::to_string),
            left_feature_group_end_time: query.left_feature_group_end_time().map(str::to_string),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FeatureStoreQueryDTO {
    href: Option<String>,
    pub(crate) query: String,
    pub(crate) query_online: String,
    pub(crate) pit_query: Option<String>,
    pub(crate) pit_query_asof: Option<String>,
    pub(crate) hudi_cached_feature_groups: Vec<FeatureGroupDTO>,
    pub(crate) on_demand_feature_groups: Vec<FeatureGroupDTO>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JoinQueryDTO {
    query: QueryDTO,
    #[serde(rename = "type")]
    join_type: JoinType,
    on: Option<Vec<String>>,
    left_on: Option<Vec<String>>,
    right_on: Option<Vec<String>>,
    prefix: Option<String>,
}

impl From<JoinQuery> for JoinQueryDTO {
    fn from(join_query: JoinQuery) -> Self {
        Self {
            query: QueryDTO::from(join_query.query),
            on: join_query.on,
            left_on: join_query.left_on,
            right_on: join_query.right_on,
            join_type: join_query.join_type,
            prefix: join_query.prefix,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryFilterDTO {
    pub(crate) feature: FeatureDTO,
    pub(crate) condition: QueryFilterCondition,
    pub(crate) value: serde_json::Value,
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

impl From<QueryFilterDTO> for QueryFilter {
    fn from(query_filter: QueryFilterDTO) -> Self {
        QueryFilter::new(
            query_filter.value,
            query_filter.condition,
            Feature::from(query_filter.feature),
        )
    }
}

impl From<QueryLogicDTO> for QueryLogic {
    fn from(query_logic: QueryLogicDTO) -> Self {
        QueryLogic::new(
            query_logic.logic_type,
            query_logic
                .left_logic
                .map(|logic| Box::new(QueryLogic::from(*logic))),
            query_logic
                .right_logic
                .map(|logic| Box::new(QueryLogic::from(*logic))),
            query_logic
                .left_filter
                .map(|filter| QueryFilter::from(filter)),
            query_logic
                .right_filter
                .map(|filter| QueryFilter::from(filter)),
        )
    }
}

impl From<QueryFilterOrLogicDTO> for QueryFilterOrLogic {
    fn from(query_filter_or_logic: QueryFilterOrLogicDTO) -> Self {
        match query_filter_or_logic {
            QueryFilterOrLogicDTO::Logic(logic) => {
                QueryFilterOrLogic::Logic(QueryLogic::from(logic))
            }
            QueryFilterOrLogicDTO::Filter(filter) => {
                QueryFilterOrLogic::Filter(QueryFilter::from(filter))
            }
        }
    }
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
