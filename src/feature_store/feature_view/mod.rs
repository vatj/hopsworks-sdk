//! Feature View API
//!
//! Feature Views serve as the main read interface of the Feature Store.
//! They are built by selecting and joining Feature from different Feature Groups.
//! The Feature View does not hold data itself, but is a logical view of the data.
//! Ideally each model should have its own Feature View that matches the schema for its input.
//!
//! Feature Views are used to generate training datasets or serve feature vectors.
//! Training datasets can be either materialized to a file on-disk or loaded directly in-memory.
//! Different training dataset can be created for the same Feature View by specifying a start and end date.
//!
//! Feature Views can also be used to retrieve feature vectors for inference pipelines.

pub mod training_dataset;
pub mod transformation_function;

use color_eyre::Result;
use serde::{Deserialize, Serialize};

use crate::{
    api::feature_store::query::entities::Query,
    core::feature_store::training_dataset::{
        create_train_test_split, create_training_dataset_attached_to_feature_view,
    },
    repositories::feature_store::feature_view::entities::FeatureViewDTO,
};
use std::collections::HashMap;

use self::transformation_function::TransformationFunction;

impl FeatureView {
    pub async fn create_train_test_split(
        &self,
        // train_start: &str,
        // train_end: &str,
        // test_start: &str,
        // test_end: &str,
        // data_format: &str,
        // coalesce: bool,
    ) -> Result<()> {
        create_train_test_split().await?;
        Ok(())
    }

    pub async fn create_attached_training_dataset(
        &self,
        // start: &str,
        // end: &str,
        // data_format: &str,
        // coalesce: bool,
    ) -> Result<()> {
        create_training_dataset_attached_to_feature_view(self).await?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FeatureView {
    pub id: i32,
    pub name: String,
    pub version: i32,
    pub query: Query,
    pub transformation_functions: HashMap<String, TransformationFunction>,
    pub feature_store_id: i32,
    pub feature_store_name: String,
}

impl From<FeatureViewDTO> for FeatureView {
    fn from(dto: FeatureViewDTO) -> Self {
        Self {
            id: dto.id,
            name: dto.name,
            version: dto.version,
            query: Query::from(dto.query),
            transformation_functions: HashMap::<String, TransformationFunction>::new(),
            feature_store_id: dto.featurestore_id,
            feature_store_name: dto.featurestore_name,
        }
    }
}
