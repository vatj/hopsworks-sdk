use color_eyre::Result;

use crate::domain::training_dataset;

use super::entities::FeatureView;

impl FeatureView {
    pub async fn create_train_test_split(
        &self,
        train_start: &str,
        train_end: &str,
        test_start: &str,
        test_end: &str,
        data_format: &str,
        coalesce: bool,
    ) -> Result<()> {
        training_dataset::controller::create_train_test_split();
        Ok(())
    }
}
