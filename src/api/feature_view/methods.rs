use color_eyre::Result;

use crate::domain::training_dataset;

use super::entities::FeatureView;

impl FeatureView {
    pub async fn create_train_test_split(
        &self,
        // train_start: &str,
        // train_end: &str,
        // test_start: &str,
        // test_end: &str,
        // data_format: &str,
        // coalesce: bool,
    ) -> Result<()> {
        training_dataset::controller::create_train_test_split().await?;
        Ok(())
    }

    pub async fn create_attached_training_dataset(
        &self,
        // start: &str,
        // end: &str,
        // data_format: &str,
        // coalesce: bool,
    ) -> Result<()> {
        training_dataset::controller::create_training_dataset_attached_to_feature_view(self)
            .await?;
        Ok(())
    }
}
