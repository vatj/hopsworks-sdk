use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub mod service;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FeatureStoreKafkaConnectorDTO {
    #[serde(rename = "type")]
    pub _type: String,
    pub bootstrap_servers: String,
    pub security_protocol: String,
    pub ssl_endpoint_identification_algorithm: String,
    pub options: Vec<String>,
    pub external_kafka: bool,
    pub id: i32,
    pub description: String,
    pub name: String,
    #[serde(rename = "featurestoreId")]
    pub feature_store_id: i32,
    pub storage_connector_type: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FeatureStoreHopsfsConnectorDTO {
    #[serde(rename = "type")]
    _type: String,
    id: i32,
    description: String,
    name: String,
    #[serde(rename = "featurestoreId")]
    feature_store_id: i32,
    storage_connector_type: String,
    hopsfs_path: String,
    dataset_name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FeatureStoreJdbcConnectorDTO {
    #[serde(rename = "type")]
    _type: String,
    id: i32,
    description: String,
    name: String,
    #[serde(rename = "featurestoreId")]
    feature_store_id: i32,
    storage_connector_type: String,
    pub(crate) connection_string: String,
    pub(crate) arguments: Vec<HashMap<String, String>>,
}

pub enum StorageConnectorDTO {
    JdbcConnectorDTO(FeatureStoreJdbcConnectorDTO),
    HopsfsConnectorDTO(FeatureStoreHopsfsConnectorDTO),
}

impl std::fmt::Debug for StorageConnectorDTO {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageConnectorDTO::JdbcConnectorDTO(jdbc_connector_dto) => jdbc_connector_dto.fmt(f),
            StorageConnectorDTO::HopsfsConnectorDTO(hopsfs_connector_dto) => {
                hopsfs_connector_dto.fmt(f)
            }
        }
    }
}

impl Serialize for StorageConnectorDTO {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match *self {
            StorageConnectorDTO::JdbcConnectorDTO(ref jdbc_connector_dto) => {
                jdbc_connector_dto.serialize(serializer)
            }
            StorageConnectorDTO::HopsfsConnectorDTO(ref hopsfs_connector_dto) => {
                hopsfs_connector_dto.serialize(serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for StorageConnectorDTO {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value: serde_json::Value = serde_json::Value::deserialize(deserializer)?;
        let _type = value["type"].as_str().unwrap();
        match _type {
            "JDBC" => {
                let jdbc_connector_dto: FeatureStoreJdbcConnectorDTO =
                    serde_json::from_value(value).unwrap();
                Ok(StorageConnectorDTO::JdbcConnectorDTO(jdbc_connector_dto))
            }
            "HOPSFS" => {
                let hopsfs_connector_dto: FeatureStoreHopsfsConnectorDTO =
                    serde_json::from_value(value).unwrap();
                Ok(StorageConnectorDTO::HopsfsConnectorDTO(
                    hopsfs_connector_dto,
                ))
            }
            _ => Err(serde::de::Error::custom(format!(
                "Unknown storage connector type: {}",
                _type,
            ))),
        }
    }
}

impl Clone for StorageConnectorDTO {
    fn clone(&self) -> Self {
        match self {
            StorageConnectorDTO::JdbcConnectorDTO(jdbc_connector_dto) => {
                StorageConnectorDTO::JdbcConnectorDTO(jdbc_connector_dto.clone())
            }
            StorageConnectorDTO::HopsfsConnectorDTO(hopsfs_connector_dto) => {
                StorageConnectorDTO::HopsfsConnectorDTO(hopsfs_connector_dto.clone())
            }
        }
    }
}
