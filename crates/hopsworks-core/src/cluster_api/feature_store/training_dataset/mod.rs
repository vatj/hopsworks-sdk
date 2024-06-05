use serde::{Deserialize, Serialize};

use crate::cluster_api::{
    feature_store::{
        feature_view::TagsDTO, query::QueryDTO,
        statistics_config::StatisticsConfigDTO,
    },
    platform::users::UserDTO,
};

pub mod payloads;
pub mod service;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum TrainingDatasetDataFormat {
    CSV,
    TSV,
    Parquet,
    Avro,
    ORC,
    TFRecord,
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

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum TrainingDatasetType {
    #[serde(rename = "HOPSFS_TRAINING_DATASET")]
    HopsFS,
    #[serde(rename = "EXTERNAL_TRAINING_DATASET")]
    External,
    #[serde(rename = "IN_MEMORY_TRAINING_DATASET")]
    InMemory,
}



#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TrainingDatasetDTO {
    #[serde(rename = "type")]
    dto_type: String,
    href: Option<String>,
    pub id: i32,
    pub name: String,
    pub version: i32,
    pub featurestore_id: i32,
    pub featurestore_name: String,
    description: Option<String>,
    query: Option<QueryDTO>,
    created: String,
    creator: UserDTO,
    statistics_config: StatisticsConfigDTO,
    tags: Option<TagsDTO>,
}
