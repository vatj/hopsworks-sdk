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
    pit_query: String,
    hudi_cached_feature_group: Vec<FeatureGroupDTO>,
    on_demand_feature_group: Vec<FeatureGroupDTO>,
}
