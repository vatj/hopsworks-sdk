use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SingleEntryPayload {
    feature_store_name: String,
    feature_view_name: String,
    feature_view_version: i32,
    entries: serde_json::Value,
    passed_features: Option<serde_json::Value>,
    metadata_options: Option<MetadataOptions>,
    options: Option<EntryOptions>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BatchEntriesPayload {
    feature_store_name: String,
    feature_view_name: String,
    feature_view_version: i32,
    entries: Vec<serde_json::Value>,
    passed_features: Vec<serde_json::Value>,
    metadata_options: Option<MetadataOptions>,
    options: Option<EntryOptions>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
struct MetadataOptions {
    feature_name: bool,
    feature_type: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
struct EntryOptions {
    validate_passed_features: bool,
    include_detailed_status: bool,
}
