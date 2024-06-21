use serde::{Deserialize, Serialize};

use crate::cluster_api::feature_store::{feature::FeatureDTO, statistics_config::StatisticsConfigDTO};
use crate::cluster_api::platform::users::UserDTO;
use crate::feature_store::feature_group::FeatureGroup;

pub mod payloads;
pub mod service;

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
    pub event_time: Option<String>,
}

impl From<&FeatureGroup> for FeatureGroupDTO {
    fn from(feature_group: &FeatureGroup) -> Self {
        FeatureGroupDTO {
            id: feature_group.id().unwrap_or(0),
            online_topic_name: feature_group
                .online_topic_name()
                .map(|online_topic_name| online_topic_name.to_string()),
            creator: match feature_group.creator() {
                Some(user) => UserDTO::from(user.clone()),
                None => panic!("creator field should not be None for an initialized FeatureGroup"),
            },
            location: feature_group.location().unwrap_or("").to_string(),
            statistics_config: Some(match feature_group.statistics_config() {
                Some(statistics_config) => StatisticsConfigDTO::from(statistics_config),
                None => panic!(
                    "statistics_config field should not be None for an initialized FeatureGroup"
                ),
            }),
            features: feature_group
                .features()
                .iter()
                .map(FeatureDTO::from)
                .collect(),
            feature_group_type: match feature_group.feature_group_type() {
                "STREAM_FEATURE_GROUP" => "streamFeatureGroupDTO".to_owned(),
                "streamFeatureGroupDTO" => "streamFeatureGroupDTO".to_owned(),
                _ => "streamFeatureGroupDTO".to_owned(),
            },
            featurestore_id: feature_group.feature_store_id(),
            featurestore_name: feature_group.feature_store_name().to_string(),
            description: feature_group
                .description()
                .map(|description| description.to_string()),
            created: feature_group.created().to_string(),
            version: feature_group.version(),
            name: feature_group.name().to_string(),
            event_time: feature_group.event_time().map(|event_time| event_time.to_string()),
            online_enabled: feature_group.is_online_enabled(),
            time_travel_format: feature_group.time_travel_format().to_string(),
        }
    }
}
