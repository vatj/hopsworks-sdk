use std::collections::HashMap;

use super::entities::FeatureStore;
use crate::api::feature_group::FeatureGroup;
use crate::api::feature_view::training_dataset::TrainingDataset;
use crate::api::feature_view::transformation_function::TransformationFunction;
use crate::api::feature_view::FeatureView;
use crate::api::query::entities::Query;
use crate::domain::{feature_group, feature_view, training_dataset, transformation_function};
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
        online_enabled: bool,
    ) -> Result<FeatureGroup> {
        if let Some(feature_group) = self
            .get_feature_group_by_name_and_version(name, version)
            .await?
        {
            return Ok(feature_group);
        }

        // If FG does not exist in backend, create a local Feature Group entity not registered with Hopsworks
        Ok(FeatureGroup::new_local(
            self,
            name,
            version,
            description,
            primary_key,
            event_time,
            online_enabled,
        ))
    }

    pub async fn create_feature_view(
        &self,
        name: &str,
        version: i32,
        query: Query,
        transformation_functions: HashMap<String, TransformationFunction>,
    ) -> Result<FeatureView> {
        feature_view::controller::create_feature_view(
            self.featurestore_id,
            self.featurestore_name.clone(),
            name.to_owned(),
            version,
            query,
            transformation_functions,
        )
        .await
    }

    pub async fn get_feature_view(
        &self,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<FeatureView>> {
        feature_view::controller::get_feature_view_by_name_and_version(
            self.featurestore_id,
            name,
            version,
        )
        .await
    }

    pub async fn get_transformation_function(
        &self,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<TransformationFunction>> {
        transformation_function::controller::get_transformation_function_by_name_and_version(
            self.featurestore_id,
            name,
            version,
        )
        .await
    }

    pub async fn get_training_dataset_by_name_and_version(
        &self,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<TrainingDataset>> {
        training_dataset::controller::get_training_dataset_by_name_and_version(
            self.featurestore_id,
            name,
            version,
        )
        .await
    }
}
