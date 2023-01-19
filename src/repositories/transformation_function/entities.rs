use serde::{Deserialize, Serialize};

use crate::api::transformation_function::entities::TransformationFunction;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TransformationFunctionResponse {
    pub href: Option<String>,
    pub count: i32,
    pub items: Vec<TransformationFunctionDTO>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TransformationFunctionDTO {
    pub id: i32,
    pub name: String,
    pub version: i32,
    pub source_code_content: String,
    pub output_type: String,
    pub featurestore_id: i32,
}

impl From<TransformationFunction> for TransformationFunctionDTO {
    fn from(transformation_function: TransformationFunction) -> Self {
        Self {
            id: transformation_function.id,
            name: transformation_function.name,
            version: transformation_function.version,
            source_code_content: transformation_function.source_code_content,
            output_type: transformation_function.output_type,
            featurestore_id: transformation_function.featurestore_id,
        }
    }
}
