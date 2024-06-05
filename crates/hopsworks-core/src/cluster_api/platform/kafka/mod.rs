use serde::{Deserialize, Serialize};

pub mod service;

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
pub struct KafkaSubjectDTO {
    pub id: i32,
    subject: String,
    pub version: i32,
    pub schema: String,
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
