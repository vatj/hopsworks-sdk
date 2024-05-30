use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewProjectPayload {
    name: String,
    description: Option<String>,
}

impl NewProjectPayload {
    pub fn new(name: &str, description: &Option<&str>) -> Self {
        Self {
            name: name.to_owned(),
            description: description.map(|s| s.to_owned()),
        }
    }
}
