use serde::{Deserialize, Serialize};

use crate::{
    feature_store::query::{join::JoinType, JoinQuery, Query, QueryFilterOrLogic},
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
    pub filters: Option<Vec<QueryFilterOrLogic>>,
}

impl From<Query> for QueryDTO {
    fn from(query: Query) -> Self {
        Self {
            href: None,
            feature_store_name: String::from(query.feature_store_name()),
            feature_store_id: query.feature_store_id(),
            left_feature_group: FeatureGroupDTO::from(query.left_feature_group.clone()),
            left_features: query
                .left_features
                .iter()
                .map(|feature| FeatureDTO::from(feature.clone()))
                .collect(),
            joins: match query.joins {
                Some(joins) => Some(
                    joins
                        .iter()
                        .map(|join| JoinQueryDTO::from(join.clone()))
                        .collect(),
                ),
                None => None,
            },
            filters: query.filters,
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
    on: Option<Vec<String>>,
    left_on: Option<Vec<String>>,
    right_on: Option<Vec<String>>,
    #[serde(rename = "type")]
    join_type: JoinType,
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
    pub(crate) operator: String,
    pub(crate) value: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryLogicDTO {
    pub logic_type: String,
    pub left_logic: Option<Box<QueryLogicDTO>>,
    pub right_logic: Option<Box<QueryLogicDTO>>,
    pub left_filter: Option<QueryFilterDTO>,
    pub right_filter: Option<QueryFilterDTO>,
}

pub enum QueryLogicOrFilterDTO {
    Logic(QueryLogicDTO),
    Filter(QueryFilterDTO),
}
