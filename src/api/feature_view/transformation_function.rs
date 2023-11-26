use serde::{Deserialize, Serialize};

use crate::repositories::transformation_function::entities::TransformationFunctionDTO;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransformationFunction {
    pub id: i32,
    pub name: String,
    pub version: i32,
    pub source_code_content: String,
    pub output_type: String,
    pub featurestore_id: i32,
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
