use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrainingDataset {
    pub feature_store_name: String,
    pub version: i32,
}
