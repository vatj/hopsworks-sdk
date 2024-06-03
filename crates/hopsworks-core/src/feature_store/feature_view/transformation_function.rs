//! Transformation Function
//!
//! This module contains the [`TransformationFunction`] entity and its related methods.
//! As of now there is no concrete plan to support this feature in the Rust API, check out the official
//! [Hopsworks Python client](https://github.com/logicalclocks/hopsworks-api) to make full use of this functionality.
use serde::{Deserialize, Serialize};

use crate::cluster_api::feature_store::transformation_function::TransformationFunctionDTO;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransformationFunction {
    pub(crate) id: i32,
    pub(crate) name: String,
    pub(crate) version: i32,
    pub(crate) source_code_content: String,
    pub(crate) output_type: String,
    pub(crate) featurestore_id: i32,
}

impl From<TransformationFunctionDTO> for TransformationFunction {
    fn from(dto: TransformationFunctionDTO) -> Self {
        Self {
            id: dto.id,
            name: dto.name,
            version: dto.version,
            source_code_content: dto.source_code_content,
            output_type: dto.output_type,
            featurestore_id: dto.featurestore_id,
        }
    }
}

impl From<&TransformationFunction> for TransformationFunctionDTO {
    fn from(transformation_function: &TransformationFunction) -> Self {
        Self {
            id: transformation_function.id,
            name: transformation_function.name.clone(),
            version: transformation_function.version,
            source_code_content: transformation_function.source_code_content.clone(),
            output_type: transformation_function.output_type.clone(),
            featurestore_id: transformation_function.featurestore_id,
        }
    }
}
