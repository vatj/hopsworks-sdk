use color_eyre::Result;
use polars::frame::DataFrame;
use serde::{Deserialize, Serialize};

use crate::{
    feature_store::{
        feature_group::statistics_config::StatisticsConfig, query::builder::BatchQueryOptions,
    },
    FeatureView,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrainingDataset {
    feature_store_name: String,
    version: i32,
}

impl TrainingDataset {
    pub fn new(feature_store_name: &str, version: i32) -> Self {
        Self {
            feature_store_name: String::from(feature_store_name),
            version,
        }
    }

    pub fn feature_store_name(&self) -> &str {
        self.feature_store_name.as_str()
    }

    pub fn version(&self) -> i32 {
        self.version
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrainingDatasetBuilder {
    feature_view_name: String,
    feature_view_version: i32,
    batch_query_options: BatchQueryOptions,
    location: Option<String>,
    seed: Option<i32>,
    statistics_config: Option<StatisticsConfig>,
    write_options: Option<serde_json::Value>,
    data_format: Option<String>,
    description: Option<String>,
    coalesce: bool,
}

impl TrainingDatasetBuilder {
    pub fn new_default_from_feature_view(feature_view: &FeatureView) -> Self {
        Self {
            feature_view_name: feature_view.name().to_string(),
            feature_view_version: feature_view.version(),
            batch_query_options: BatchQueryOptions::default(),
            location: None,
            seed: None,
            statistics_config: None,
            write_options: None,
            data_format: None,
            description: None,
            coalesce: false,
        }
    }

    async fn register(&self) -> Result<TrainingDataset> {
        crate::core::feature_store::training_dataset::register_training_dataset(self).await
    }

    pub async fn materialize_on_cluster(&self) -> Result<TrainingDataset> {
        crate::core::feature_store::training_dataset::materialize_on_cluster(self).await
    }

    pub async fn read_from_offline_feature_store(&self) -> Result<(TrainingDataset, DataFrame)> {
        let training_dataset = self.register().await?;
        let df = crate::core::feature_store::training_dataset::read_from_offline_feature_store(
            &training_dataset,
        )
        .await?;

        Ok((training_dataset, df))
    }
}
