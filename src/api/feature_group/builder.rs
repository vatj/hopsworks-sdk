use super::entities::{Feature, FeatureGroup};
use crate::repositories::{
    feature::entities::FeatureDTO, feature_group::entities::FeatureGroupDTO,
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
