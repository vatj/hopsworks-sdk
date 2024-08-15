use serde::{Deserialize, Serialize};

pub mod service;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StringMessageResponse {
    pub success_message: String,
}