use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicDTO {
    href: String,
    pub(crate) name: String,
    schema_name: String,
    schema_version: i32,
    shared: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct KafkaSubjectDTO {
    pub(crate) id: i32,
    subject: String,
    pub(crate) version: i32,
    pub(crate) schema: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicListDTO {
    href: String,
    pub items: Vec<KafkaTopicDTO>,
    pub count: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct KafkaBrokersDTO {
    href: String,
    pub brokers: Vec<String>,
}
