use serde::{Deserialize, Serialize};

use crate::crate::feature_store::feature::payloads::NewFeaturePayload;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewFeatureGroupPayload {
    #[serde(rename = "type")]
    fg_type: String,
    name: String,
    version: i32,
    description: Option<String>,
    features: Vec<NewFeaturePayload>,
    event_time: Option<String>,
    online_enabled: bool,
}

impl NewFeatureGroupPayload {
    pub fn new(
        name: &str,
        version: i32,
        description: Option<&str>,
        features: Vec<NewFeaturePayload>,
        event_time: Option<&str>,
        online_enabled: bool,
    ) -> Self {
        Self {
            fg_type: String::from("streamFeatureGroupDTO"),
            name: String::from(name),
            version,
            description: description.map(String::from),
            features,
            event_time: event_time.map(String::from),
            online_enabled,
        }
    }
}
