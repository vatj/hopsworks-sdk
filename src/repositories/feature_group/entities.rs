use serde::{Deserialize, Serialize};

use crate::repositories::{
    feature::entities::FeatureDTO, statistics_config::entities::StatisticsConfigDTO,
    users::entities::UserDTO,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FeatureGroupDTO {
    #[serde(rename = "type")]
    pub feature_group_type: String,
    pub featurestore_id: i32,
    pub featurestore_name: String,
    pub description: Option<String>,
    pub created: String,
    pub creator: UserDTO,
    pub version: i32,
    pub name: String,
    pub id: i32,
    pub location: String,
    pub statistics_config: Option<StatisticsConfigDTO>,
    pub features: Vec<FeatureDTO>,
    pub online_enabled: bool,
    pub time_travel_format: String,
    pub online_topic_name: Option<String>,
}
