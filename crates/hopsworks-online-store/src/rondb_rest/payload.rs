use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SingleEntryPayload {
    pub(crate) feature_store_id: i32,
    pub(crate) feature_view_name: String,
    pub(crate) feature_view_version: i32,
    pub(crate) entries: serde_json::Value,
    pub(crate) passed_values: Option<serde_json::Value>,
    pub(crate) metadata_options: Option<MetadataOptionsPayload>,
    pub(crate) options: Option<OptionsPayload>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchEntriesPayload {
    pub(crate) feature_store_id: i32,
    pub(crate) feature_view_name: String,
    pub(crate) feature_view_version: i32,
    pub(crate) entries: Vec<serde_json::Value>,
    pub(crate) passed_values: Vec<serde_json::Value>,
    pub(crate) metadata_options: Option<MetadataOptionsPayload>,
    pub(crate) options: Option<OptionsPayload>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MetadataOptionsPayload {
    pub(crate) feature_name: bool,
    pub(crate) feature_type: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OptionsPayload {
    pub(crate) validate_passed_features: bool,
    pub(crate) detailed_status: bool,
}