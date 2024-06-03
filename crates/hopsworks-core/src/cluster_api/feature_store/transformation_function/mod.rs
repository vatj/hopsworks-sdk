use serde::{Deserialize, Serialize};

pub mod service;

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
