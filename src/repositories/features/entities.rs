use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FeatureDTO {
    pub name: String,
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub data_type: String,
    pub primary: bool,
    pub partition: bool,
    pub hudi_precombine_key: bool,
    pub feature_group_id: i32,
}
