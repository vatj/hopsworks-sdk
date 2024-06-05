use serde::{Deserialize, Serialize};

pub mod service;


#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CredentialsDTO {
    pub file_extension: String,
    pub t_store: String,
    pub k_store: String,
    pub password: String,
    pub ca_chain: String,
    pub client_key: String,
    pub client_cert: String,
}
