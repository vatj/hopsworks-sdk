use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewProjectPayload<'a> {
    name: &'a str,
    description: Option<&'a str>,
}
