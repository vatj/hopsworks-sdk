use color_eyre::Result;

use crate::repositories::feature_store::{self, entities::FeatureStoreDTO};

pub async fn get_default_feature_store(
    project_id: i32,
    project_name: &str,
) -> Result<FeatureStoreDTO> {
    let feature_store_name = format!("{project_name}_featurestore");
    feature_store::service::get_feature_store_by_project_id_and_name(
        project_id,
        feature_store_name.as_str(),
    )
    .await
}
