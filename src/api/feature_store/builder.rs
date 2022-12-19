use super::entities::FeatureStore;
use crate::repositories::feature_store::entities::FeatureStoreDTO;

impl From<FeatureStoreDTO> for FeatureStore {
    fn from(feature_store_dto: FeatureStoreDTO) -> Self {
        FeatureStore::new_from_dto(feature_store_dto)
    }
}
