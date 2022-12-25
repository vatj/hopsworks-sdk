use serde::{Deserialize, Serialize};

use crate::repositories::{
    job::entities::JobExecutionUserDTO, statistics_config::entities::StatisticsConfigDTO,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FeatureViewResponseDTO {
    #[serde(rename = "type")]
    dto_type: String,
    href: String,
    count: Option<i32>,
    pub items: Vec<FeatureViewDTO>,
    statistics_config: StatisticsConfigDTO,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FeatureViewDTO {
    #[serde(rename = "type")]
    dto_type: String,
    href: String,
    id: i32,
    name: String,
    version: i32,
    featurestore_id: i32,
    featurestore_name: String,
    description: String,
    created: String,
    creator: JobExecutionUserDTO,
    statistics_config: StatisticsConfigDTO,
    tags: TagDTO,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TagDTO {
    href: String,
}
