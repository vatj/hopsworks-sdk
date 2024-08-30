use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::embedding_feature::EmbeddingFeature;
use crate::cluster_api::feature_store::embedding::EmbeddingIndexDTO;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EmbeddingIndexMetadata {
    pub index_name: String,
    pub col_prefix: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EmbeddingIndex {
    #[serde(flatten)]
    pub metadata: EmbeddingIndexMetadata,
    pub features: Option<HashMap<String, EmbeddingFeature>>,
}

impl EmbeddingIndexMetadata {
    pub fn new(index_name: String, col_prefix: Option<String>) -> Self {
        Self {
            index_name,
            col_prefix,
        }
    }
}

impl EmbeddingIndex {
    pub fn new(index_name: &str) -> Self {
        Self {
            metadata: EmbeddingIndexMetadata::new(index_name.to_string(), None),
            features: None,
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn add_embedding_feature(&mut self, name: &str, embedding_feature: EmbeddingFeature) {
        if self.features.is_none() {
            self.features = Some(HashMap::new());
        }
        self.features.as_mut().unwrap().insert(String::from(name), embedding_feature);
    }
}

impl From<EmbeddingIndexDTO> for EmbeddingIndex {
    fn from(embedding_index_dto: EmbeddingIndexDTO) -> Self {
        let features: Option<HashMap<String, EmbeddingFeature>> = match embedding_index_dto.features {
            Some(features) => {
                let features = features
                    .into_iter()
                    .map(|(name, feature)| (name.clone(), feature.into()))
                    .collect();
                Some(features)
            }
            None => None,
        };
        Self {
            metadata: EmbeddingIndexMetadata::new(embedding_index_dto.index_name.clone(), embedding_index_dto.col_prefix.clone()),
            features,
        }
    }
}