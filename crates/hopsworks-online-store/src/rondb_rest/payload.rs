use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SingleEntryPayload {
    feature_store_id: i32,
    feature_view_name: String,
    feature_view_version: i32,
    entries: serde_json::Value,
    passed_values: Option<serde_json::Value>,
    metadata_options: MetadataOptionsPayload,
    options: OptionsPayload,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BatchEntriesPayload {
    feature_store_id: i32,
    feature_view_name: String,
    feature_view_version: i32,
    entries: Vec<serde_json::Value>,
    passed_values: Vec<serde_json::Value>,
    metadata_options: MetadataOptionsPayload,
    options: OptionsPayload,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MetadataOptionsPayload {
    feature_name: bool,
    feature_type: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OptionsPayload {
    validate_passed_features: bool,
    detailed_status: bool,
}