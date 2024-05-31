//! Feature View API
//!
//! [`Feature View`][FeatureView]s serve as the main read interface of the [`Feature Store`][crate::feature_store::FeatureStore].
//! They are built by selecting and joining [`Feature`][crate::feature_store::feature_group::feature::Feature] from different
//! [`Feature Group`][crate::feature_store::FeatureGroup]s. The [`Feature View`][FeatureView] does not hold data itself, but is a logical view of the data.
//! Ideally each model should have its own Feature View that matches the schema for its input.

pub mod training_dataset;
pub mod training_dataset_builder;
pub mod transformation_function;

use color_eyre::Result;
use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::{
    core::feature_store::{
        query::{read_query_from_online_feature_store, read_with_arrow_flight_client},
        training_dataset::{
            create_train_test_split, create_training_dataset_attached_to_feature_view,
        },
    },
    feature_store::query::Query,
    hopsworks_internal::feature_store::feature_view::entities::FeatureViewDTO,
};
use std::collections::HashMap;

use self::{training_dataset_builder::NoSplit, transformation_function::TransformationFunction};

use super::query::{
    builder::BatchQueryOptions,
    read_option::{self, OfflineReadOptions, OnlineReadOptions},
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FeatureView {
    // Feature Views serve as the main read interface of the Feature Store.
    // Feature Views are used to generate training datasets or serve feature vectors.
    // Training datasets can be either materialized to a file on-disk or loaded directly in-memory.
    // Different training dataset can be created for the same Feature View by specifying a start and end date.
    //
    // Feature Views can also be used to retrieve feature vectors for inference pipelines,
    // for either batch or real-time. In batch mode, the feature view can read a batch of data from
    // the offline Feature Store and return the transformed dataframe. You can use the same feature view
    // which was used to create the dataset to trained the corresponding model. This guarantees compatibility
    // between feature vectors and model input, making the transition between training and serving pipelines seamless.
    //
    // If all Feature Groups used in the Feature View are online-enabled, then the Feature View can also be used
    // to serve feature vectors in real-time. Providing the primary keys of the Feature Groups to the Feature View
    // will return the latest feature vector for each primary key. This is useful for real-time inference pipelines
    // that require low latency. It enables model which relies on user data being augmented by pre-processed Feature data
    // to be deployed in production.
    //
    // ---
    // **:warning: Not Implemented:** Transformation Functions are not yet supported in the Rust API. Check out the official Python client to
    // make full use of this fonctionality.
    //
    // ---
    //
    // # Examples
    //
    // ## Create a Feature View
    //
    // ```no_run
    // use color_eyre::Result;
    //
    // #[tokio::main]
    // async fn main() -> Result<()> {
    //   let fs = hopsworks::login(None).await?.get_feature_store().await?;
    //
    //   let fg_1 = fs.get_feature_group_by_name_and_version("my_fg_1", 1).await?.unwrap();
    //   let fg_2 = fs.get_feature_group_by_name_and_version("my_fg_2", 1).await?.unwrap();
    //   let query = fg_1.select(&["feature_1", "feature_2"])?.join(
    //     fg_2.select(&["feature_3", "feature_4"])?,
    //     None,
    //   );
    //
    //  let feature_view = fs.create_feature_view(
    //    "my_feature_view",
    //    1,
    //    query,
    //    None,
    //  ).await?;
    //
    // Ok(())
    // }
    // ```
    //
    // ## Create a Training Dataset
    //
    // ```no_run
    // use color_eyre::Result;
    //
    // #[tokio::main]
    // async fn main() -> Result<()> {
    //  let fs = hopsworks::login(None).await?.get_feature_store().await?;
    //  let feature_view = fs.get_feature_view("my_feature_view", Some(1)).await?.unwrap();
    //
    //  let training_dataset = feature_view.create_attached_training_dataset().await?;
    //
    // Ok(())
    // }
    // ```
    //
    // ## Create and Return In-memory Training Dataset as a Polars DataFrame
    //
    // ```no_run
    // use color_eyre::Result;
    //
    // #[tokio::main]
    // async fn main() -> Result<()> {
    //   let fs = hopsworks::login(None).await?.get_feature_store().await?;
    //   let feature_view = fs.get_feature_view("my_feature_view", Some(1)).await?.unwrap();
    //
    //   let training_dataset_dataframe = feature_view.read_from_offline_feature_store(None).await?;
    //
    //   Ok(())
    // }
    // ```
    //
    // ## Read from Online Feature Store
    //
    // > Note: This feature is not yet supported in the Rust API. Check out the official Python client to
    // make full use of this fonctionality.
    id: i32,
    name: String,
    version: i32,
    query: Query,
    transformation_functions: HashMap<String, TransformationFunction>,
    feature_store_id: i32,
    feature_store_name: String,
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

impl FeatureView {
    pub fn feature_store_id(&self) -> i32 {
        self.feature_store_id
    }

    pub(crate) fn feature_store_name(&self) -> &str {
        self.feature_store_name.as_ref()
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub fn version(&self) -> i32 {
        self.version
    }

    pub fn query(&self) -> &Query {
        &self.query
    }

    pub fn query_mut(&mut self) -> &mut Query {
        &mut self.query
    }

    pub fn transformation_functions(&self) -> &HashMap<String, TransformationFunction> {
        &self.transformation_functions
    }

    pub fn transformation_functions_mut(&mut self) -> &mut HashMap<String, TransformationFunction> {
        &mut self.transformation_functions
    }

    pub async fn get_batch_query_string(
        &self,
        batch_query_options: &BatchQueryOptions,
    ) -> Result<String> {
        crate::core::feature_store::feature_view::get_batch_query_string(self, batch_query_options)
            .await
    }

    pub async fn get_batch_query(&self, batch_query_options: &BatchQueryOptions) -> Result<Query> {
        crate::core::feature_store::feature_view::get_batch_query(self, batch_query_options).await
    }

    pub async fn get_batch_data(
        &self,
        batch_query_options: &BatchQueryOptions,
        offline_read_options: Option<read_option::OfflineReadOptions>,
    ) -> Result<DataFrame> {
        crate::core::feature_store::feature_view::get_batch_data(
            self,
            batch_query_options,
            offline_read_options,
        )
        .await
    }

    pub fn training_dataset_builder(
        &self,
    ) -> self::training_dataset_builder::TrainingDatasetBuilder<NoSplit> {
        self::training_dataset_builder::TrainingDatasetBuilder::new_default_from_feature_view(
            self.feature_store_id(),
            self.name(),
            self.version(),
        )
    }

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

    pub async fn read_from_offline_feature_store(
        &self,
        offline_read_options: Option<OfflineReadOptions>,
    ) -> Result<DataFrame> {
        read_with_arrow_flight_client(self.query.clone(), offline_read_options).await
    }

    pub async fn read_from_online_feature_store(
        &self,
        online_read_options: Option<OnlineReadOptions>,
    ) -> Result<DataFrame> {
        read_query_from_online_feature_store(&self.query.clone(), online_read_options).await
    }
}
