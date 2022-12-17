use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FeatureGroup {
    featurestore_id: i32,
    featurestore_name: String,
    feature_group_type: String,
    description: String,
    created: String,
    creator: User,
    version: i32,
    name: String,
    id: i32,
    location: String,
    statistics_config: StatisticsConfig,
    features: Vec<Feature>,
    online_enabled: bool,
    time_travel_format: String,
    pub online_topic_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct User {
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
pub struct Feature {
    name: String,
    description: Option<String>,
    data_type: String,
    primary: bool,
    partition: bool,
    hudi_precombine_key: bool,
    feature_group_id: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StatisticsConfig {
    enabled: bool,
    histograms: bool,
    correlations: bool,
    exact_uniqueness: bool,
    columns: Vec<String>,
}
