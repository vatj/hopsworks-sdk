use chrono::{DateTime, Utc};
use color_eyre::Result;
use polars::frame::DataFrame;
use serde::{Deserialize, Serialize};

use crate::{
    feature_store::{
        feature_group::statistics_config::StatisticsConfig, query::QueryFilterOrLogic,
    },
    repositories::feature_store::{
        storage_connector::entities::StorageConnectorDTO,
        training_dataset::payloads::TrainingDatasetSplitSizes,
    },
};

use super::training_dataset::TrainingDataset;

mod seal {
    pub trait Sealed {}
    impl Sealed for super::NoSplit {}
    impl Sealed for super::TestSplit {}
    impl Sealed for super::TestValidationSplit {}
}

pub trait TrainingDatasetBuilderState: seal::Sealed {}
impl TrainingDatasetBuilderState for NoSplit {}
impl TrainingDatasetBuilderState for TestSplit {}
impl TrainingDatasetBuilderState for TestValidationSplit {}

pub struct NoSplit;
pub struct TestSplit;
pub struct TestValidationSplit;

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub(crate) struct SplitOptions {
    pub(crate) split_start_time: Option<DateTime<Utc>>,
    pub(crate) split_end_time: Option<DateTime<Utc>>,
    pub(crate) split_size: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum TrainingDatasetDataFormat {
    CSV,
    TSV,
    Parquet,
    Avro,
    ORC,
    TFRecord,
}

impl SplitOptions {
    fn with_start_time(mut self, split_start_time: DateTime<Utc>) -> Self {
        self.split_start_time = Some(split_start_time);
        self
    }

    fn with_end_time(mut self, split_end_time: DateTime<Utc>) -> Self {
        self.split_end_time = Some(split_end_time);
        self
    }

    fn with_size(mut self, split_size: f64) -> Self {
        self.split_size = Some(split_size);
        self
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrainingDatasetBuilder<State>
where
    State: TrainingDatasetBuilderState,
{
    pub(crate) feature_store_id: i32,
    pub(crate) feature_view_name: String,
    pub(crate) feature_view_version: i32,
    pub(crate) location: Option<String>,
    pub(crate) seed: Option<i32>,
    pub(crate) extra_filters: Option<Vec<QueryFilterOrLogic>>,
    pub(crate) statistics_config: Option<StatisticsConfig>,
    pub(crate) write_options: Option<serde_json::Value>,
    pub(crate) data_format: Option<TrainingDatasetDataFormat>,
    pub(crate) description: Option<String>,
    pub(crate) coalesce: bool,
    pub(crate) train_split_options: SplitOptions,
    pub(crate) test_split_options: Option<SplitOptions>,
    pub(crate) validation_split_options: Option<SplitOptions>,
    pub(crate) storage_connector: Option<StorageConnectorDTO>,
    state: std::marker::PhantomData<State>,
}

impl TrainingDatasetBuilder<NoSplit> {
    pub fn new_default_from_feature_view(
        feature_store_id: i32,
        feature_view_name: &str,
        feature_view_version: i32,
    ) -> Self {
        Self {
            feature_store_id,
            feature_view_name: feature_view_name.to_string(),
            feature_view_version,
            train_split_options: SplitOptions::default(),
            test_split_options: None,
            validation_split_options: None,
            extra_filters: None,
            location: None,
            seed: None,
            statistics_config: None,
            write_options: None,
            data_format: None,
            description: None,
            coalesce: false,
            storage_connector: None,
            state: std::marker::PhantomData::<NoSplit>,
        }
    }

    pub fn with_test_split(self) -> TrainingDatasetBuilder<TestSplit> {
        TrainingDatasetBuilder {
            feature_store_id: self.feature_store_id,
            feature_view_name: self.feature_view_name,
            feature_view_version: self.feature_view_version,
            test_split_options: Some(SplitOptions::default()),
            validation_split_options: None,
            train_split_options: self.train_split_options,
            location: self.location,
            seed: self.seed,
            statistics_config: self.statistics_config,
            write_options: self.write_options,
            data_format: self.data_format,
            description: self.description,
            coalesce: self.coalesce,
            extra_filters: self.extra_filters,
            storage_connector: self.storage_connector,
            state: std::marker::PhantomData::<TestSplit>,
        }
    }

    pub fn with_test_validation_split(self) -> TrainingDatasetBuilder<TestValidationSplit> {
        TrainingDatasetBuilder {
            feature_store_id: self.feature_store_id,
            feature_view_name: self.feature_view_name,
            feature_view_version: self.feature_view_version,
            test_split_options: Some(SplitOptions::default()),
            validation_split_options: Some(SplitOptions::default()),
            train_split_options: self.train_split_options,
            location: self.location,
            seed: self.seed,
            statistics_config: self.statistics_config,
            write_options: self.write_options,
            data_format: self.data_format,
            description: self.description,
            coalesce: self.coalesce,
            extra_filters: self.extra_filters,
            storage_connector: self.storage_connector,
            state: std::marker::PhantomData::<TestValidationSplit>,
        }
    }
}

impl TrainingDatasetBuilder<TestSplit> {
    pub fn with_test_size(mut self, test_size: f64) -> Self {
        self.test_split_options = self.test_split_options.map(|o| o.with_size(test_size));
        self
    }

    pub fn with_test_start_time(mut self, test_start_time: DateTime<Utc>) -> Self {
        self.test_split_options = self
            .test_split_options
            .map(|o| o.with_start_time(test_start_time));
        self
    }

    pub fn with_test_end_time(mut self, test_end_time: DateTime<Utc>) -> Self {
        self.test_split_options = self
            .test_split_options
            .map(|o| o.with_end_time(test_end_time));
        self
    }
}

impl TrainingDatasetBuilder<TestValidationSplit> {
    pub fn with_validation_size(mut self, validation_size: f64) -> Self {
        self.validation_split_options = self
            .validation_split_options
            .map(|o| o.with_size(validation_size));
        self
    }

    pub fn with_validation_start_time(mut self, validation_start_time: DateTime<Utc>) -> Self {
        self.validation_split_options = self
            .validation_split_options
            .map(|o| o.with_start_time(validation_start_time));
        self
    }

    pub fn with_validation_end_time(mut self, validation_end_time: DateTime<Utc>) -> Self {
        self.validation_split_options = self
            .validation_split_options
            .map(|o| o.with_end_time(validation_end_time));
        self
    }

    pub fn with_test_size(mut self, test_size: f64) -> Self {
        self.test_split_options = self.test_split_options.map(|o| o.with_size(test_size));
        self
    }

    pub fn with_test_start_time(mut self, test_start_time: DateTime<Utc>) -> Self {
        self.test_split_options = self
            .test_split_options
            .map(|o| o.with_start_time(test_start_time));
        self
    }

    pub fn with_test_end_time(mut self, test_end_time: DateTime<Utc>) -> Self {
        self.test_split_options = self
            .test_split_options
            .map(|o| o.with_end_time(test_end_time));
        self
    }
}

impl<State> TrainingDatasetBuilder<State>
where
    State: TrainingDatasetBuilderState,
{
    pub fn with_single_file_per_split(mut self) -> Self {
        self.coalesce = true;
        self
    }

    pub fn with_description(mut self, description: &str) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_data_format(mut self, data_format: TrainingDatasetDataFormat) -> Self {
        self.data_format = Some(data_format);
        self
    }

    pub fn with_write_options(mut self, write_options: serde_json::Value) -> Self {
        self.write_options = Some(write_options);
        self
    }

    pub fn with_statistics_config(mut self, statistics_config: StatisticsConfig) -> Self {
        self.statistics_config = Some(statistics_config);
        self
    }

    pub fn with_seed(mut self, seed: i32) -> Self {
        self.seed = Some(seed);
        self
    }

    pub fn with_train_start_time(mut self, start_time: DateTime<Utc>) -> Self {
        self.train_split_options = self.train_split_options.with_start_time(start_time);
        self
    }

    pub fn with_train_end_time(mut self, end_time: DateTime<Utc>) -> Self {
        self.train_split_options = self.train_split_options.with_end_time(end_time);
        self
    }

    pub(crate) fn get_split_sizes(&self) -> TrainingDatasetSplitSizes {
        let test_split_size = self
            .test_split_options
            .as_ref()
            .and_then(|o| o.split_size)
            .unwrap_or(0.0);
        let validation_split_size = self
            .validation_split_options
            .as_ref()
            .and_then(|o| o.split_size)
            .unwrap_or(0.0);
        let train_split_size = 1.0 - test_split_size - validation_split_size;

        TrainingDatasetSplitSizes::new(train_split_size, test_split_size, validation_split_size)
    }

    async fn register(&self) -> Result<TrainingDataset> {
        crate::core::feature_store::training_dataset::register_training_dataset(self).await
    }

    pub async fn materialize_on_cluster(&self) -> Result<TrainingDataset> {
        crate::core::feature_store::training_dataset::materialize_on_cluster(self).await
    }

    pub async fn read_from_offline_feature_store(&self) -> Result<(TrainingDataset, DataFrame)> {
        let training_dataset = self.register().await?;
        let df = crate::core::feature_store::training_dataset::read_from_offline_feature_store(
            &training_dataset,
        )
        .await?;

        Ok((training_dataset, df))
    }
}
