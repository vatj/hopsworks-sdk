use super::entities::{Feature, FeatureGroup, StatisticsConfig, User};
use crate::repositories::{
    feature::entities::FeatureDTO, feature_group::entities::FeatureGroupDTO,
    statistics_config::entities::StatisticsConfigDTO, users::entities::UserDTO,
};

impl From<FeatureGroupDTO> for FeatureGroup {
    fn from(feature_group_dto: FeatureGroupDTO) -> Self {
        FeatureGroup::new_from_dto(feature_group_dto)
    }
}

impl From<FeatureDTO> for Feature {
    fn from(feature_dto: FeatureDTO) -> Self {
        Feature::new_from_dto(feature_dto)
    }
}

impl From<UserDTO> for User {
    fn from(user_dto: UserDTO) -> Self {
        User::new_from_dto(user_dto)
    }
}

impl From<StatisticsConfigDTO> for StatisticsConfig {
    fn from(statistics_config_dto: StatisticsConfigDTO) -> Self {
        StatisticsConfig::new_from_dto(statistics_config_dto)
    }
}
