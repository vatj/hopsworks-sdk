use serde::{Deserialize, Serialize};

use crate::{
    api::query::entities::Query,
    repositories::{feature::entities::FeatureDTO, feature_group::entities::FeatureGroupDTO},
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
}

impl From<Query> for QueryDTO {
    fn from(query: Query) -> Self {
        Self {
            href: None,
            feature_store_name: query.feature_store_name.clone(),
            feature_store_id: query.feature_store_id,
            left_feature_group: FeatureGroupDTO::from(query.left_feature_group.clone()),
            left_features: query
                .left_features
                .iter()
                .map(|feature| FeatureDTO::from(feature.clone()))
                .collect(),
            joins: Some(vec![]),
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
pub struct JoinQueryDTO {}
