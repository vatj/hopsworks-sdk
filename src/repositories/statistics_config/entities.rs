use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StatisticsConfigDTO {
    pub enabled: bool,
    pub histograms: bool,
    pub correlations: bool,
    pub exact_uniqueness: bool,
    pub columns: Vec<String>,
}
