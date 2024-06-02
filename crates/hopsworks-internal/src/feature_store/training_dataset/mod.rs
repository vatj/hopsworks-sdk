use serde::{Deserialize, Serialize};

use crate::{
    feature_store::{
        feature_view::TagsDTO, query::QueryDTO,
        statistics_config::StatisticsConfigDTO,
    },
    platform::users::UserDTO,
};

pub mod payloads;
pub mod service;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TrainingDatasetDTO {
    #[serde(rename = "type")]
    dto_type: String,
    href: Option<String>,
    pub id: i32,
    pub name: String,
    pub version: i32,
    pub featurestore_id: i32,
    pub featurestore_name: String,
    description: Option<String>,
    query: Option<QueryDTO>,
    created: String,
    creator: UserDTO,
    statistics_config: StatisticsConfigDTO,
    tags: Option<TagsDTO>,
}
