use super::entities::Project;
use crate::api::feature_store::entities::FeatureStore;
use crate::domain::feature_store;
use color_eyre::Result;

impl Project {
    pub async fn get_feature_store(&self) -> Result<FeatureStore> {
        Ok(FeatureStore::from(
            feature_store::controller::get_project_default_feature_store(
                self.id,
                self.project_name.as_str(),
            )
            .await?,
        ))
    }
}
