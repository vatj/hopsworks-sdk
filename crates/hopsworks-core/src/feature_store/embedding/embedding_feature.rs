use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::feature_store::FeatureGroup;
use crate::cluster_api::feature_store::embedding::EmbeddingFeatureDTO;

use super::embedding_index::EmbeddingIndexMetadata;
use super::{SimilarityFunction, MinimalModelMetadata};


#[derive(Serialize, Deserialize, Debug, Clone, TypedBuilder)]
pub struct EmbeddingFeature {
    pub name: String,
    pub dimension: u32,
    #[builder(default = SimilarityFunction::L2)]
    pub similarity_function: SimilarityFunction,
    #[builder(default)]
    pub model: Option<MinimalModelMetadata>,
    #[builder(default)]
    pub feature_group: Option<FeatureGroup>,
    #[builder(default)]
    pub embedding_index: Option<EmbeddingIndexMetadata>,
}

impl EmbeddingFeature {
    pub fn set_model(&mut self, model: MinimalModelMetadata) {
        self.model = Some(model);
    }

    pub fn set_feature_group(&mut self, feature_group: FeatureGroup) {
        self.feature_group = Some(feature_group);
    }

    pub fn set_embedding_index(&mut self, embedding_index: EmbeddingIndexMetadata) {
        self.embedding_index = Some(embedding_index);
    }
}

impl From<EmbeddingFeatureDTO> for EmbeddingFeature {
    fn from(embedding_feature_dto: EmbeddingFeatureDTO) -> Self {
        Self {
            name: embedding_feature_dto.name.clone(),
            dimension: embedding_feature_dto.dimension,
            similarity_function: embedding_feature_dto.similarity_function.clone(),
            model: None,
            feature_group: None,
            embedding_index: None,
        }
    }
}