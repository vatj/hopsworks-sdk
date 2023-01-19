use serde::{Deserialize, Serialize};

use crate::repositories::{
    query::entities::QueryDTO, statistics_config::entities::StatisticsConfigDTO,
    users::entities::UserDTO,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FeatureViewResponseDTO {
    #[serde(rename = "type")]
    dto_type: String,
    href: Option<String>,
    count: Option<i32>,
    pub items: Vec<FeatureViewDTO>,
    statistics_config: StatisticsConfigDTO,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FeatureViewDTO {
    #[serde(rename = "type")]
    dto_type: String,
    href: Option<String>,
    pub id: i32,
    pub name: String,
    pub version: i32,
    pub featurestore_id: i32,
    pub featurestore_name: String,
    pub query: QueryDTO,
    description: Option<String>,
    created: String,
    creator: UserDTO,
    statistics_config: StatisticsConfigDTO,
    tags: TagsDTO,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TagsDTO {
    href: Option<String>,
}
