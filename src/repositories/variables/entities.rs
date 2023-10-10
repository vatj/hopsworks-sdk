use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BoolMessageResponse {
    pub success_message: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StringMessageResponse {
    pub success_message: String,
}

