use super::entities::FeatureStore;
use crate::api::feature_group::entities::FeatureGroup;
use crate::domain::feature_group;
use color_eyre::Result;

impl FeatureStore {
    pub async fn get_feature_group_by_name_and_version(
        &self,
        name: &str,
        version: i32,
    ) -> Result<Option<FeatureGroup>> {
        if let Some(feature_group_dto) =
            feature_group::controller::get_feature_group_by_name_and_version(
                self.project_id,
                self.featurestore_id,
                name,
                version,
            )
            .await?
        {
            Ok(Some(FeatureGroup::from(feature_group_dto)))
        } else {
            Ok(None)
        }
    }
}
