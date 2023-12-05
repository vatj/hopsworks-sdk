use color_eyre::Result;

use crate::repositories::{
    feature_store::{self, entities::FeatureStoreDTO},
    platform::project::{entities::ProjectDTO, service::get_project_and_user_list},
};

pub async fn get_default_feature_store(project_name: &str) -> Result<FeatureStoreDTO> {
    let feature_store_name = format!("{project_name}_featurestore");
    feature_store::service::get_feature_store_by_name(feature_store_name.as_str()).await
}

pub async fn get_project_list() -> Result<Vec<ProjectDTO>> {
    Ok(get_project_and_user_list()
        .await?
        .iter()
        .map(|project_and_user| project_and_user.project.to_owned())
        .collect())
}
