use color_eyre::Result;

use crate::repositories::feature_group::{self, entities::FeatureGroupDTO};

pub async fn get_feature_group_by_name_and_version(
    project_id: i32,
    feature_store_id: i32,
    name: &str,
    version: i32,
) -> Result<Option<FeatureGroupDTO>> {
    feature_group::service::get_feature_group_by_name_and_version(
        project_id,
        feature_store_id,
        name,
        version,
    )
    .await
}
