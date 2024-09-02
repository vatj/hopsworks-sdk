use serde::{Deserialize, Serialize};

use crate::cluster_api::feature_store::{
    feature::TrainingDatasetFeatureDTO,
    query::{FeatureStoreQueryDTO, QueryDTO},
};

use super::{KeywordDTO, TagsDTO};

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
        description: Option<&str>,
    ) -> Self {
        Self {
            dto_type: "featureViewDTO".to_owned(),
            name: String::from(name),
            version,
            query,
            query_string: query_string.cloned(),
            featurestore_id: feature_store_id,
            featurestore_name: String::from(feature_store_name),
            description: description.map(String::from),
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
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
    pub td_version: Option<i32>,
    pub with_label: bool,
    pub with_primary_keys: bool,
    pub with_event_time: bool,
    pub training_helper_columns: bool,
    pub inference_helper_columns: bool,
    pub is_hive_engine: bool,
}
