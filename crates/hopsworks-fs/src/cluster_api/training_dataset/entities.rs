use serde::{Deserialize, Serialize};

use crate::cluster_api::{
    feature_store::{
        feature_view::entities::TagsDTO, query::entities::QueryDTO,
        statistics_config::entities::StatisticsConfigDTO,
    },
    platform::users::UserDTO,
};

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
