use serde::{Deserialize, Serialize};

use crate::repositories::features::payloads::NewFeaturePayload;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewFeatureGroupPayload<'a> {
    name: &'a str,
    version: i32,
    description: Option<&'a str>,
    features: Vec<NewFeaturePayload<'a>>,
    event_time: Option<&'a str>,
    primary_key: Vec<&'a str>,
}

impl<'a> NewFeatureGroupPayload<'a> {
    pub fn new(
        name: &'a str,
        version: i32,
        description: Option<&'a str>,
        features: Vec<NewFeaturePayload<'a>>,
        primary_key: Vec<&'a str>,
        event_time: Option<&'a str>,
    ) -> Self {
        Self {
            name,
            version,
            description,
            features,
            event_time,
            primary_key,
        }
    }
}
