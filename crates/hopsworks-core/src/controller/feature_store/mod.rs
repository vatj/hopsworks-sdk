pub mod feature;
pub mod feature_group;
pub mod feature_view;
pub mod query;
pub mod storage_connector;
pub mod training_dataset;
pub mod transformation_function;

use color_eyre::Result;

use crate::hopsworks_internal::feature_store::{self, FeatureStoreDTO};

pub async fn get_project_default_feature_store(project_name: &str) -> Result<FeatureStoreDTO> {
    let feature_store_name = format!("{project_name}_featurestore");
    feature_store::service::get_feature_store_by_name(feature_store_name.as_str()).await
}
