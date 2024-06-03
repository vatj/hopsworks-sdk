use color_eyre::Result;
use polars::prelude::DataFrame;

use hopsworks_core::feature_store::feature_view::{training_dataset::TrainingDataset, training_dataset_builder::TrainingDatasetBuilder};

pub async fn read_from_offline_feature_store(
    _training_dataset: &TrainingDataset,
) -> Result<DataFrame> {
    todo!("read_from_offline_feature_store is not implemented");
}

pub async fn read_from_offline_feature_store(training_dataset_builder: &TrainingDatasetBuilder<S>) -> Result<(TrainingDataset, DataFrame)> {
        let training_dataset = training_dataset_builder.register().await?;
        let df = crate::controller::feature_store::training_dataset::read_from_offline_feature_store(
            &training_dataset,
        )
        .await?;

        Ok((training_dataset, df))
    }