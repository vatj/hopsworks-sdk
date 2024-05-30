use serde::{Deserialize, Serialize};

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

// Intended for arrow flight client
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RegisterArrowFlightClientCertificatePayload {
    tstore: String,
    kstore: String,
    cert_key: String,
}

impl RegisterArrowFlightClientCertificatePayload {
    pub fn new(tstore: String, kstore: String, cert_key: String) -> Self {
        Self {
            tstore,
            kstore,
            cert_key,
        }
    }
}
