use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserDTO {
    email: String,
    first_name: String,
    last_name: String,
    status: i32,
    tos: bool,
    two_factor: bool,
    tours_state: i32,
    max_num_projects: i32,
    num_created_projects: i32,
    test_user: bool,
    num_active_projects: i32,
    num_remaining_projects: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FeatureDTO {
    name: String,
    description: Option<String>,
    #[serde(rename = "type")]
    data_type: String,
    primary: bool,
    partition: bool,
    hudi_precombine_key: bool,
    feature_group_id: i32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatisticsConfigDTO {
    enabled: bool,
    histograms: bool,
    correlations: bool,
    exact_uniqueness: bool,
    columns: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureGroupDTO {
    #[serde(rename = "type")]
    feature_group_type: String,
    featurestore_id: i32,
    featurestore_name: String,
    description: String,
    created: String,
    creator: UserDTO,
    version: i32,
    name: String,
    id: i32,
    location: String,
    statistics_config: StatisticsConfigDTO,
    features: Vec<FeatureDTO>,
    online_enabled: bool,
    time_travel_format: String,
    pub online_topic_name: Option<String>,
}
