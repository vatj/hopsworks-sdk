use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    feature_store::feature_view::training_dataset_builder::{
        TrainingDatasetBuilder, TrainingDatasetDataFormat,
    },
    repositories::feature_store::{
        feature::entities::TrainingDatasetFeatureDTO,
        feature_view::entities::{KeywordDTO, TagsDTO},
        query::entities::{FeatureStoreQueryDTO, QueryDTO},
        statistics_config::entities::StatisticsConfigDTO,
        storage_connector::entities::StorageConnectorDTO,
    },
};

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum TrainingDatasetType {
    #[serde(rename = "HOPSFS_TRAINING_DATASET")]
    HopsFS,
    #[serde(rename = "EXTERNAL_TRAINING_DATASET")]
    External,
    #[serde(rename = "IN_MEMORY_TRAINING_DATASET")]
    InMemory,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct TrainingDatasetSplitSizes {
    train: f64,
    test: f64,
    validation: f64,
}

impl TrainingDatasetSplitSizes {
    pub fn new(train: f64, test: f64, validation: f64) -> Self {
        Self {
            train,
            test,
            validation,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewTrainingDatasetPayloadV2 {
    #[serde(rename = "type")]
    pub dto_type: Arc<str>,
    pub featurestore_id: i32,
    pub event_start_time: Option<DateTime<Utc>>,
    pub event_end_time: Option<DateTime<Utc>>,
    pub description: Option<Arc<str>>,
    pub version: Option<i32>,
    pub name: Arc<str>,
    pub training_dataset_type: Option<TrainingDatasetType>,
    pub data_format: Option<TrainingDatasetDataFormat>,
    pub coalesce: bool,
    pub statistics_config: Option<StatisticsConfigDTO>,
    pub train_split: Option<Arc<str>>,
    pub location: Option<Arc<str>>,
    pub splits: Option<TrainingDatasetSplitSizes>,
    pub storage_connector: Option<StorageConnectorDTO>,
}

impl From<&TrainingDatasetBuilder> for NewTrainingDatasetPayloadV2 {
    fn from(builder: &TrainingDatasetBuilder) -> Self {
        let (train_split, split_sizes) =
            if builder.validation_split_options.is_some() || builder.test_split_options.is_some() {
                (Some("train".into()), Some(builder.get_split_sizes()))
            } else {
                (None, None)
            };

        Self {
            dto_type: "trainingDatasetDTO".into(),
            name: builder.feature_view_name.into(),
            version: None,
            training_dataset_type: None,
            data_format: builder.data_format,
            coalesce: builder.coalesce,
            featurestore_id: builder.feature_store_id,
            description: builder.description,
            location: builder.location.into(),
            event_end_time: builder.batch_query_options.end_time,
            event_start_time: builder.batch_query_options.start_time,
            train_split,
            splits: split_sizes,
            storage_connector: builder.storage_connector,
            statistics_config: builder
                .statistics_config
                .as_ref()
                .map(StatisticsConfigDTO::from),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewTrainingDatasetPayload {
    #[serde(rename = "type")]
    pub dto_type: String,
    pub featurestore_id: i32,
    pub featurestore_name: String,
    pub description: Option<String>,
    pub version: i32,
    pub name: String,
    pub training_dataset_type: String,
    pub data_format: String,
    pub coalesce: bool,
    pub location: String,
    pub features: Vec<TrainingDatasetFeatureDTO>,
    pub query: QueryDTO,
    pub query_string: Option<FeatureStoreQueryDTO>,
    pub keywords: Option<KeywordDTO>,
    pub tags: Option<TagsDTO>,
}

impl NewTrainingDatasetPayload {
    pub fn new(
        feature_store_id: i32,
        feature_store_name: String,
        name: String,
        version: i32,
        query: QueryDTO,
        query_string: Option<FeatureStoreQueryDTO>,
        features: Vec<TrainingDatasetFeatureDTO>,
    ) -> Self {
        Self {
            dto_type: "trainingDatasetDTO".to_owned(),
            name,
            version,
            query,
            query_string,
            training_dataset_type: "HOPSFS_TRAINING_DATASET".to_owned(),
            data_format: "csv".to_owned(),
            coalesce: true,
            featurestore_id: feature_store_id,
            featurestore_name: feature_store_name,
            description: None,
            location: "".to_owned(),
            features,
            keywords: None,
            tags: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TrainingDatasetComputeJobConfigPayload {
    pub overwrite: bool,
    pub write_options: Vec<OptionDTO>,
    pub spark_job_configuration: Option<SparkJobConfiguration>,
    pub query: QueryDTO,
}

impl TrainingDatasetComputeJobConfigPayload {
    pub fn new(overwrite: bool, query: QueryDTO) -> Self {
        Self {
            overwrite,
            write_options: vec![],
            spark_job_configuration: None,
            query,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OptionDTO {
    name: String,
    value: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SparkJobConfiguration {
    #[serde(rename = "type")]
    spark_job_configuration_type: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrainingDatasetArrowFlightPayload {
    pub fs_name: String,
    pub fv_name: String,
    pub fv_version: i32,
    pub tds_version: i32,
    pub tds_query: String,
}

impl TrainingDatasetArrowFlightPayload {
    pub fn new(
        fs_name: String,
        fv_name: String,
        fv_version: i32,
        tds_version: i32,
        tds_query: String,
    ) -> Self {
        Self {
            fs_name,
            fv_name,
            fv_version,
            tds_version,
            tds_query,
        }
    }
}
