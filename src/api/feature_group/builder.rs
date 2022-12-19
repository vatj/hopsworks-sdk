use super::entities::FeatureGroup;
use crate::repositories::feature_group::entities::FeatureGroupDTO;

impl From<FeatureGroupDTO> for FeatureGroup {
    fn from(feature_group_dto: FeatureGroupDTO) -> Self {
        FeatureGroup::new_from_dto(feature_group_dto)
    }
}
