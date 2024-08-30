use serde::{Deserialize, Serialize};

pub mod embedding_feature;
pub mod embedding_index;

pub use embedding_feature::EmbeddingFeature;
pub use embedding_index::EmbeddingIndex;

type SimilarityFunction = crate::cluster_api::feature_store::embedding::SimilarityFunctionDTO;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinimalModelMetadata {
    pub model_registry_id: i32,
    pub model_version: i32,
    pub model_name: String,
}