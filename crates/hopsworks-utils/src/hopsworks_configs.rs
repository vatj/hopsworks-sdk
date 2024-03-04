use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration Templates for Hopsworks CLI

/// Hopsworks User Configuration (e.g. `user1` or `service_account_1`)
#[derive(Debug, Serialize, Deserialize)]
pub struct HopsworksUserConfig {
    pub api_key: String,
    pub name: Option<String>,
}

/// Hopswoks Project Configuration (e.g. `my_project` or `sandbox_project`)
#[derive(Debug, Serialize, Deserialize)]
pub struct HopsworksProjectConfig {
    pub name: String,
}

/// Hopsworks Cluster Configuration (e.g. `prod-cluster` or `sandbox`)
#[derive(Debug, Serialize, Deserialize)]
pub struct HopsworksClusterConfig {
    /// IP address or domain name of the Hopsworks cluster
    pub host: String,
    /// Port to use the hopsworks API
    pub port: u16,
}

/// Hopsworks Profile Configuration (e.g. `prod-user1` or `sandbox-service_account_1`)
/// A profile is a combination of a cluster and a user configuration
#[derive(Debug, Serialize, Deserialize)]
pub struct HopsworksProfileConfig {
    /// Name of the cluster configuration in toml config file (e.g. `prod-cluster` or `sandbox-cluster`)
    pub cluster: HopsworksClusterConfig,
    /// Name of the user configuration in toml config file (e.g. `me_prod` or `admin_service_account_sandbox`)
    pub user: HopsworksUserConfig,
    /// Name of the project configuration in toml config file (e.g. `my_project` or `sandbox_project`)
    pub project: HopsworksProjectConfig,
}

/// Hopsworks TOML Configuration
///
/// This is the main configuration file for the Hopsworks CLI and SDK
/// It contains the default profiles, clusters and users and sets the default to be used,
/// when no environment variable is set.
#[derive(Debug, Serialize, Deserialize)]
pub struct HopsworksTomlConfig {
    /// Name of the default profile to be used, when no environment variable is set
    pub default_profile: Option<String>,
    /// List of profiles to apply when running the CLI
    pub profiles: HashMap<String, HopsworksProfileConfig>,
}
