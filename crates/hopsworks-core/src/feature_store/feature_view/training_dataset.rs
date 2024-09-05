use serde::{Deserialize, Serialize};

use crate::cluster_api::feature_store::training_dataset::TrainingDatasetDTO;

pub use crate::cluster_api::feature_store::training_dataset::TrainingDatasetDataFormat;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrainingDataset {
    feature_store_name: String,
    version: i32,
}

impl TrainingDataset {
    pub fn new(feature_store_name: &str, version: i32) -> Self {
        Self {
            feature_store_name: String::from(feature_store_name),
            version,
        }
    }

    pub fn feature_store_name(&self) -> &str {
        self.feature_store_name.as_str()
    }

    pub fn version(&self) -> i32 {
        self.version
    }
}

impl From<&TrainingDatasetDTO> for TrainingDataset {
    fn from(training_dataset_dto: &TrainingDatasetDTO) -> Self {
        Self::new(
            &training_dataset_dto.featurestore_name,
            training_dataset_dto.version,
        )
    }
}
