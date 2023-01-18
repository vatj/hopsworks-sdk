use super::entities::FeatureStore;
use crate::api::feature_group::entities::FeatureGroup;
use crate::api::transformation_function::entities::TransformationFunction;
use crate::domain::feature_group;
use crate::domain::transformation_function::controller::get_transformation_function_by_name_and_version;
use color_eyre::Result;

impl FeatureStore {
    pub async fn get_feature_group_by_name_and_version(
        &self,
        name: &str,
        version: i32,
    ) -> Result<Option<FeatureGroup>> {
        if let Some(feature_group_dto) =
            feature_group::controller::get_feature_group_by_name_and_version(
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

    pub async fn get_or_create_feature_group(
        &self,
        name: &str,
        version: i32,
        description: Option<&str>,
        primary_key: Vec<&str>,
        event_time: &str,
    ) -> Result<FeatureGroup> {
        if let Some(feature_group) = self
            .get_feature_group_by_name_and_version(name, version)
            .await?
        {
            return Ok(feature_group);
        }

        Ok(FeatureGroup::new_local(
            self,
            name,
            version,
            description,
            primary_key,
            event_time,
        ))
    }

    pub async fn get_transformation_function(
        &self,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<TransformationFunction>> {
        get_transformation_function_by_name_and_version(self.featurestore_id, name, version).await
    }
}
