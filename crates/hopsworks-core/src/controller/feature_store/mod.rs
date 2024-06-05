pub mod feature;
pub mod feature_group;
pub mod feature_view;
pub mod query;
pub mod storage_connector;
pub mod training_dataset;
pub mod transformation_function;

use color_eyre::Result;

use crate::cluster_api::feature_store;
use crate::feature_store::FeatureStore;

pub async fn get_project_default_feature_store(project_name: &str) -> Result<FeatureStore> {
    let feature_store_name = format!("{project_name}_featurestore");
    Ok(FeatureStore::from(feature_store::service::get_feature_store_by_name(feature_store_name.as_str()).await?))
}
