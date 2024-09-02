use serde::{Deserialize, Serialize};

/// Note that the service in this module does not contact the opensearch service,
/// rather it contacts the Hopsworks service to get the opensearch auth details or
/// indexes related to an Hopsworks project. For the actual opensearch client, see
/// the `hopsworks-opensearch` module.
pub mod service;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct OpenSearchTokenDTO {
    pub token: String,
}