use serde::{Deserialize, Serialize};

/// Configuration Templates for Hopsworks CLI and SDK

/// Hopsworks Cluster Configuration (e.g. `prod-cluster` or `sandbox`)
#[derive(Debug, Serialize, Deserialize)]
pub struct HopsworksClusterConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
}

/// Hopsworks User Configuration (e.g. `user1` or `service_account_1`)
#[derive(Debug, Serialize, Deserialize)]
pub struct HopsworksUserConfig {
    pub name: String,
    pub api_key: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HopsworksProjectConfig {
    pub name: String,
    pub project_name: String,
    pub project_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HopsworksProfileConfig {
    pub name: String,
    pub cluster: String,
    pub user: String,
    pub project: String,
    pub default: bool,
}
