use serde::{Deserialize, Serialize};

use crate::repositories::feature_group::entities::FeatureGroupDTO;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryDTO {}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FeatureStoreQueryDTO {
    href: String,
    query: String,
    query_online: String,
    pit_query: Option<String>,
    hudi_cached_feature_groups: Vec<FeatureGroupDTO>,
    on_demand_feature_groups: Vec<FeatureGroupDTO>,
}
