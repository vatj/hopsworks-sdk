use serde::{Deserialize, Serialize};

use hopsworks_fs::cluster_api::feature_store::statistics_config::entities::StatisticsConfigDTO;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StatisticsConfig {
    pub enabled: bool,
    pub histograms: bool,
    pub correlations: bool,
    pub exact_uniqueness: bool,
    pub columns: Vec<String>,
}

impl StatisticsConfig {
    pub fn new_from_dto(statistics_config_dto: &StatisticsConfigDTO) -> Self {
        Self {
            enabled: statistics_config_dto.enabled,
            histograms: statistics_config_dto.histograms,
            correlations: statistics_config_dto.correlations,
            exact_uniqueness: statistics_config_dto.exact_uniqueness,
            columns: statistics_config_dto.columns.clone(),
        }
    }
}

impl From<&StatisticsConfigDTO> for StatisticsConfig {
    fn from(statistics_config_dto: &StatisticsConfigDTO) -> Self {
        StatisticsConfig::new_from_dto(statistics_config_dto)
    }
}
