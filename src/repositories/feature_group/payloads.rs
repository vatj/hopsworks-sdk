use serde::{Deserialize, Serialize};

use crate::repositories::feature::payloads::NewFeaturePayload;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewFeatureGroupPayload<'a> {
    #[serde(rename = "type")]
    fg_type: &'a str,
    name: &'a str,
    version: i32,
    description: Option<&'a str>,
    features: Vec<NewFeaturePayload>,
    event_time: Option<&'a str>,
}

impl<'a> NewFeatureGroupPayload<'a> {
    pub fn new(
        name: &'a str,
        version: i32,
        description: Option<&'a str>,
        features: Vec<NewFeaturePayload>,
        event_time: Option<&'a str>,
    ) -> Self {
        Self {
            fg_type: "streamFeatureGroupDTO",
            name,
            version,
            description,
            features,
            event_time,
        }
    }
}
