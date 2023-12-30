use serde::{Deserialize, Serialize};

use crate::{
    feature_store::query::builder::BatchQueryOptions,
    repositories::feature_store::{
        feature::entities::TrainingDatasetFeatureDTO,
        query::entities::{FeatureStoreQueryDTO, QueryDTO},
    },
};

use super::entities::{KeywordDTO, TagsDTO};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewFeatureViewPayload {
    #[serde(rename = "type")]
    pub dto_type: String,
    pub featurestore_id: i32,
    pub featurestore_name: String,
    pub description: Option<String>,
    pub version: i32,
    pub name: String,
    pub location: String,
    pub features: Vec<TrainingDatasetFeatureDTO>,
    pub query: QueryDTO,
    pub query_string: Option<FeatureStoreQueryDTO>,
    pub keywords: Option<KeywordDTO>,
    pub tags: Option<TagsDTO>,
}

impl NewFeatureViewPayload {
    pub fn new(
        feature_store_id: i32,
        feature_store_name: &str,
        name: &str,
        version: i32,
        query: QueryDTO,
        query_string: Option<&FeatureStoreQueryDTO>,
        features: Vec<TrainingDatasetFeatureDTO>,
    ) -> Self {
        Self {
            dto_type: "featureViewDTO".to_owned(),
            name: String::from(name),
            version,
            query,
            query_string: query_string.cloned(),
            featurestore_id: feature_store_id,
            featurestore_name: String::from(feature_store_name),
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
pub struct FeatureViewBatchQueryPayload {
    start_time: i64,
    end_time: i64,
    td_version: Option<i32>,
    with_label: bool,
    with_primary_keys: bool,
    with_event_time: bool,
    training_helper_columns: Vec<String>,
    inference_helper_columns: Vec<String>,
    is_hive_engine: bool,
}

impl From<&BatchQueryOptions> for FeatureViewBatchQueryPayload {
    fn from(options: &BatchQueryOptions) -> Self {
        Self {
            start_time: options.start_time.timestamp_millis(),
            end_time: options.end_time.timestamp_millis(),
            td_version: options.td_version,
            with_label: options.with_label,
            with_primary_keys: options.with_primary_keys,
            with_event_time: options.with_event_time,
            training_helper_columns: options.training_helper_columns.clone(),
            inference_helper_columns: options.inference_helper_columns.clone(),
            is_hive_engine: false,
        }
    }
}
