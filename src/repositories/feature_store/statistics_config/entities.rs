use serde::{Deserialize, Serialize};

use crate::api::feature_store::feature_group::statistics_config::StatisticsConfig;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StatisticsConfigDTO {
    pub enabled: bool,
    pub histograms: bool,
    pub correlations: bool,
    pub exact_uniqueness: bool,
    pub columns: Vec<String>,
}

impl From<StatisticsConfig> for StatisticsConfigDTO {
    fn from(statistics_config: StatisticsConfig) -> Self {
        StatisticsConfigDTO::new_from_statistics_config(statistics_config)
    }
}

impl StatisticsConfigDTO {
    pub fn new_from_statistics_config(statistics_config: StatisticsConfig) -> Self {
        Self {
            enabled: statistics_config.enabled,
            histograms: statistics_config.histograms,
            correlations: statistics_config.correlations,
            exact_uniqueness: statistics_config.exact_uniqueness,
            columns: statistics_config.columns,
        }
    }
}
