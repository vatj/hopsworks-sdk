use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewFeaturePayload {
    pub name: String,
    #[serde(rename = "type")]
    data_type: String,
    pub primary: bool,
    hudi_precombine_key: bool,
    partition: bool,
}

impl NewFeaturePayload {
    pub fn new(name: String, data_type: String) -> Self {
        Self {
            name,
            data_type,
            primary: false,
            hudi_precombine_key: false,
            partition: false,
        }
    }
}
