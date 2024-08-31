use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::feature_store::embedding::embedding_feature::EmbeddingFeature;
use crate::feature_store::embedding::embedding_index::EmbeddingIndex;

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EmbeddingFeatureDTO {
    pub name: String,
    pub dimension: u32,
    pub similarity_function: SimilarityFunctionDTO,
}

impl From<&EmbeddingFeature> for EmbeddingFeatureDTO {
    fn from(embedding_feature: &EmbeddingFeature) -> Self {
        Self {
            name: embedding_feature.name.clone(),
            dimension: embedding_feature.dimension,
            similarity_function: embedding_feature.similarity_function.clone(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EmbeddingIndexDTO {
    pub index_name: String,
    pub col_prefix: Option<String>,
    pub features: Option<HashMap<String, EmbeddingFeatureDTO>>,
}

impl From<&EmbeddingIndex> for EmbeddingIndexDTO {
    fn from(embedding_index: &EmbeddingIndex) -> Self {
        let features: Option<HashMap<String, EmbeddingFeatureDTO>> = match embedding_index.features
        {
            Some(ref features) => {
                let features = features
                    .iter()
                    .map(|(name, feature)| (name.clone(), feature.into()))
                    .collect();
                Some(features)
            }
            None => None,
        };
        Self {
            index_name: embedding_index.metadata.index_name.clone(),
            col_prefix: embedding_index.metadata.col_prefix.clone(),
            features,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimilarityFunctionDTO {
    L2,
    Cosine,
    DotProduct,
}

impl<'de> Deserialize<'de> for SimilarityFunctionDTO {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "L2" => Ok(SimilarityFunctionDTO::L2),
            "COSINE" => Ok(SimilarityFunctionDTO::Cosine),
            "DOT_PRODUCT" => Ok(SimilarityFunctionDTO::DotProduct),
            _ => Err(serde::de::Error::custom(format!(
                "unknown similarity function: {}",
                s
            ))),
        }
    }
}

impl Serialize for SimilarityFunctionDTO {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            SimilarityFunctionDTO::L2 => "L2",
            SimilarityFunctionDTO::Cosine => "COSINE",
            SimilarityFunctionDTO::DotProduct => "DOT_PRODUCT",
        };
        serializer.serialize_str(s)
    }
}
