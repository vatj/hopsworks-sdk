use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicDTO {
    href: String,
    pub name: String,
    schema_name: String,
    schema_version: i32,
    shared: bool,
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
