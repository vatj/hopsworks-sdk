use serde::{Deserialize, Serialize};

use crate::repositories::{
    feature::entities::TrainingDatasetFeatureDTO,
    feature_view::entities::{KeywordDTO, TagsDTO},
    query::entities::{FeatureStoreQueryDTO, QueryDTO},
};

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
            features: features,
            keywords: None,
            tags: None,
        }
    }
}
