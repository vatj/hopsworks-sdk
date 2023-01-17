use serde::{Deserialize, Serialize};

use crate::{
    api::feature_group::entities::FeatureGroup,
    repositories::{
        feature::entities::FeatureDTO, statistics_config::entities::StatisticsConfigDTO,
        users::entities::UserDTO,
    },
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

impl From<FeatureGroup> for FeatureGroupDTO {
    fn from(feature_group: FeatureGroup) -> Self {
        FeatureGroupDTO::new_from_feature_group(feature_group)
    }
}

impl FeatureGroupDTO {
    pub fn new_from_feature_group(feature_group: FeatureGroup) -> Self {
        Self {
            id: feature_group.get_id().unwrap_or(0),
            online_topic_name: feature_group.get_online_topic_name(),
            feature_group_type: feature_group.feature_group_type,
            featurestore_id: feature_group.featurestore_id,
            featurestore_name: feature_group.featurestore_name,
            description: feature_group.description,
            created: feature_group.created,
            creator: match feature_group.creator {
                Some(user) => UserDTO::from(user),
                None => panic!("creator field should not be None for an initialized FeatureGroup"),
            },
            version: feature_group.version,
            name: feature_group.name,
            location: feature_group.location.unwrap_or_else(|| String::from("")),
            statistics_config: Some(match feature_group.statistics_config {
                Some(statistics_config) => StatisticsConfigDTO::from(statistics_config),
                None => panic!(
                    "statistics_config field should not be None for an initialized FeatureGroup"
                ),
            }),
            features: feature_group
                .features
                .iter()
                .map(|feature| FeatureDTO::from(feature.clone()))
                .collect(),
            online_enabled: feature_group.online_enabled,
            time_travel_format: feature_group.time_travel_format,
        }
    }
}
