use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{
    cluster_api::feature_store::{
        statistics_config::StatisticsConfigDTO,
        storage_connector::StorageConnectorDTO,
        training_dataset::{payloads::NewTrainingDatasetPayloadV2, TrainingDatasetDataFormat},
    },
    feature_store::feature_group::statistics_config::StatisticsConfig,
};

#[derive(Debug, Serialize, Deserialize, Clone, TypedBuilder)]
pub struct TrainingDatasetMetadata {
    pub feature_store_id: i32,
    pub feature_view_name: String,
    pub feature_view_version: i32,
    pub description: Option<String>,
    pub data_format: Option<TrainingDatasetDataFormat>,
    pub statistics_config: Option<StatisticsConfig>,
    pub location: Option<String>,
    pub seed: Option<i64>,
    #[builder(default = false)]
    pub coalesce: bool,
    #[builder(default, setter(skip))]
    pub storage_connector: Option<StorageConnectorDTO>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SizeSplit {
    pub train: f64,
    pub test: f64,
    pub validation: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone, TypedBuilder)]
pub struct EventTimeSplit {
    pub train_start_time: Option<i64>,
    pub train_end_time: Option<i64>,
    pub test_start_time: Option<i64>,
    pub test_end_time: Option<i64>,
    pub validation_start_time: Option<i64>,
    pub validation_end_time: Option<i64>,
}
