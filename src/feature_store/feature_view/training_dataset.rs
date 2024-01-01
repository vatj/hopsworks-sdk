use serde::{Deserialize, Serialize};

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
