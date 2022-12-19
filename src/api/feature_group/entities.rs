use serde::{Deserialize, Serialize};

use crate::repositories::feature_group::entities::{
    FeatureDTO, FeatureGroupDTO, StatisticsConfigDTO, UserDTO,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FeatureGroup {
    featurestore_id: i32,
    featurestore_name: String,
    feature_group_type: String,
    description: String,
    created: String,
    creator: User,
    version: i32,
    name: String,
    id: i32,
    location: String,
    statistics_config: StatisticsConfig,
    features: Vec<Feature>,
    online_enabled: bool,
    time_travel_format: String,
    pub online_topic_name: Option<String>,
}

impl FeatureGroup {
    pub fn new_from_dto(feature_group_dto: FeatureGroupDTO) -> Self {
        Self {
            featurestore_id: feature_group_dto.featurestore_id,
            featurestore_name: feature_group_dto.featurestore_name,
            feature_group_type: feature_group_dto.feature_group_type,
            description: feature_group_dto.description,
            created: feature_group_dto.created,
            creator: User::new_from_dto(feature_group_dto.creator),
            version: feature_group_dto.version,
            name: feature_group_dto.name,
            id: feature_group_dto.id,
            location: feature_group_dto.location,
            statistics_config: StatisticsConfig::new_from_dto(feature_group_dto.statistics_config),
            features: feature_group_dto
                .features
                .iter()
                .map(|feature_dto| Feature::new_from_dto(feature_dto.to_owned()))
                .collect(),
            online_enabled: feature_group_dto.online_enabled,
            time_travel_format: feature_group_dto.time_travel_format,
            online_topic_name: feature_group_dto.online_topic_name,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct User {
    email: String,
    first_name: String,
    last_name: String,
    status: i32,
    tos: bool,
    two_factor: bool,
    tours_state: i32,
    max_num_projects: i32,
    num_created_projects: i32,
    test_user: bool,
    num_active_projects: i32,
    num_remaining_projects: i32,
}

impl User {
    pub fn new_from_dto(user_dto: UserDTO) -> Self {
        Self {
            email: user_dto.email,
            first_name: user_dto.first_name,
            last_name: user_dto.last_name,
            status: user_dto.status,
            tos: user_dto.tos,
            two_factor: user_dto.two_factor,
            tours_state: user_dto.tours_state,
            max_num_projects: user_dto.max_num_projects,
            num_created_projects: user_dto.num_created_projects,
            test_user: user_dto.test_user,
            num_active_projects: user_dto.num_active_projects,
            num_remaining_projects: user_dto.num_remaining_projects,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Feature {
    name: String,
    description: Option<String>,
    data_type: String,
    primary: bool,
    partition: bool,
    hudi_precombine_key: bool,
    feature_group_id: i32,
}

impl Feature {
    pub fn new_from_dto(feature_dto: FeatureDTO) -> Self {
        Self {
            name: feature_dto.name,
            description: feature_dto.description,
            data_type: feature_dto.data_type,
            primary: feature_dto.primary,
            partition: feature_dto.partition,
            hudi_precombine_key: feature_dto.hudi_precombine_key,
            feature_group_id: feature_dto.feature_group_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StatisticsConfig {
    enabled: bool,
    histograms: bool,
    correlations: bool,
    exact_uniqueness: bool,
    columns: Vec<String>,
}

impl StatisticsConfig {
    pub fn new_from_dto(statistics_config_dto: StatisticsConfigDTO) -> Self {
        Self {
            enabled: statistics_config_dto.enabled,
            histograms: statistics_config_dto.histograms,
            correlations: statistics_config_dto.correlations,
            exact_uniqueness: statistics_config_dto.exact_uniqueness,
            columns: statistics_config_dto.columns,
        }
    }
}
