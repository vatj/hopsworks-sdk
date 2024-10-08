use serde::{Deserialize, Serialize};

use crate::cluster_api::feature_store::{query::QueryDTO, statistics_config::StatisticsConfigDTO};
use crate::cluster_api::platform::users::UserDTO;

pub mod payloads;
pub mod service;

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
    description: String,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct KeywordDTO {}
