use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};


#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SingleFeatureVector {
    features: serde_json::Value,
    metadata: Vec<OptMetadata>,
    status: GetVectorStatus,
    detailed_status: Option<GetVectorDetailedStatus>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BatchFeatureVectors {
    features: Vec<serde_json::Value>,
    metadata: Vec<OptMetadata>,
    status: GetVectorStatus,
    detailed_status: Option<GetVectorDetailedStatus>,
}


#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OptMetadata {
    feature_name: Option<String>,
    feature_type: Option<String>,
}

#[derive(Clone, Debug, EnumString, PartialEq, Serialize, Deserialize, Display)]
pub enum GetVectorStatus {
    #[strum(serialize = "COMPLETE")]
    Complete,
    #[strum(serialize = "MISSING")]
    Missing,
    #[strum(serialize = "ERROR")]
    Error,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GetVectorDetailedStatus {
    feature_group_id: i32,
    http_status: i32,
}