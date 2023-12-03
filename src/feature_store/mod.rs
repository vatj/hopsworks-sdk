//! Feature Store to write, read and manage Feature data.
//!
//! The `FeatureStore` lies at the heart of the Hopsworks MLOps platform.
//! It is a centralized repository of Feature data that can be used both for training
//! and serving machine learning models. [`FeatureGroup`](crate::feature_store::feature_group::FeatureGroup)
//! is the perfect sink for all Feature Engineering pipelines, allowing you to easily share Features across teams and projects.
//! [`FeatureView`](crate::feature_store::feature_view::FeatureView)s allow you to group these Features
//! to serve as schema for a ML model and a convenient interface to read data from the `FeatureStore`.
//! They provide methods to read or materialize on-disk training datasets, serve Features in real-time and
//! define transformations to apply to the raw data before serving it to the model.
pub mod feature_group;
pub mod feature_view;
pub mod query;

use std::collections::HashMap;

use color_eyre::Result;

use serde::{Deserialize, Serialize};

use crate::{
    core::feature_store::{
        feature_group::get_feature_group_by_name_and_version,
        feature_view::{create_feature_view, get_feature_view_by_name_and_version},
        training_dataset::get_training_dataset_by_name_and_version,
        transformation_function::get_transformation_function_by_name_and_version,
    },
    repositories::feature_store::entities::FeatureStoreDTO,
};

use self::{
    feature_view::{
        training_dataset::TrainingDataset, transformation_function::TransformationFunction,
    },
    query::entities::Query,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FeatureStore {
    num_feature_groups: i32,
    num_training_datasets: i32,
    num_storage_connectors: i32,
    num_feature_views: i32,
    pub featurestore_id: i32,
    pub featurestore_name: String,
    created: String,
    project_name: String,
    project_id: i32,
    featurestore_description: Option<String>,
    online_featurestore_name: String,
    online_featurestore_size: Option<f64>,
    online_enabled: bool,
}

impl FeatureStore {
    pub fn new_from_dto(feature_store_dto: FeatureStoreDTO) -> Self {
        Self {
            num_feature_groups: feature_store_dto.num_feature_groups,
            num_training_datasets: feature_store_dto.num_training_datasets,
            num_storage_connectors: feature_store_dto.num_storage_connectors,
            num_feature_views: feature_store_dto.num_feature_views,
            featurestore_id: feature_store_dto.featurestore_id,
            featurestore_name: feature_store_dto.featurestore_name,
            created: feature_store_dto.created,
            project_name: feature_store_dto.project_name,
            project_id: feature_store_dto.project_id,
            featurestore_description: feature_store_dto.featurestore_description,
            online_featurestore_name: feature_store_dto.online_featurestore_name,
            online_featurestore_size: feature_store_dto.online_featurestore_size,
            online_enabled: feature_store_dto.online_enabled,
        }
    }
}

impl From<FeatureStoreDTO> for FeatureStore {
    fn from(feature_store_dto: FeatureStoreDTO) -> Self {
        FeatureStore::new_from_dto(feature_store_dto)
    }
}

impl FeatureStore {
    pub async fn get_feature_group_by_name_and_version(
        &self,
        name: &str,
        version: i32,
    ) -> Result<Option<feature_group::FeatureGroup>> {
        if let Some(feature_group_dto) =
            get_feature_group_by_name_and_version(self.featurestore_id, name, version).await?
        {
            Ok(Some(feature_group::FeatureGroup::from(feature_group_dto)))
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
        event_time: Option<&str>,
        online_enabled: bool,
    ) -> Result<feature_group::FeatureGroup> {
        if let Some(feature_group) = self
            .get_feature_group_by_name_and_version(name, version)
            .await?
        {
            return Ok(feature_group);
        }

        // If FG does not exist in backend, create a local Feature Group entity not registered with Hopsworks
        Ok(self.create_feature_group(
            name,
            version,
            description,
            primary_key,
            event_time,
            online_enabled,
        ))
    }

    pub fn create_feature_group(
        &self,
        name: &str,
        version: i32,
        description: Option<&str>,
        primary_key: Vec<&str>,
        event_time: Option<&str>,
        online_enabled: bool,
    ) -> feature_group::FeatureGroup {
        feature_group::FeatureGroup::new_local(
            self,
            name,
            version,
            description,
            primary_key,
            event_time,
            online_enabled,
        )
    }

    pub async fn create_feature_view(
        &self,
        name: &str,
        version: i32,
        query: Query,
        transformation_functions: Option<HashMap<String, TransformationFunction>>,
    ) -> Result<feature_view::FeatureView> {
        create_feature_view(
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
    ) -> Result<Option<feature_view::FeatureView>> {
        get_feature_view_by_name_and_version(self.featurestore_id, name, version).await
    }

    pub async fn get_transformation_function(
        &self,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<TransformationFunction>> {
        get_transformation_function_by_name_and_version(self.featurestore_id, name, version).await
    }

    pub async fn get_training_dataset_by_name_and_version(
        &self,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<TrainingDataset>> {
        get_training_dataset_by_name_and_version(self.featurestore_id, name, version).await
    }
}
