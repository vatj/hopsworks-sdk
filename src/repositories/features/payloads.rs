use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewFeaturePayload<'a> {
    name: &'a str,
    description: Option<&'a str>,
    #[serde(rename = "type")]
    data_type: &'a str,
}

impl<'a> NewFeaturePayload<'a> {
    pub fn new(name: &'a str, data_type: &'a str, description: Option<&'a str>) -> Self {
        Self {
            name,
            description,
            data_type,
        }
    }
}
