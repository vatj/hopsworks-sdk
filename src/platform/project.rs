use color_eyre::Result;
use serde::{Deserialize, Serialize};

use crate::api::feature_store::FeatureStore;
use crate::core::feature_store;
use crate::repositories::platform::project::entities::ProjectDTO;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Project {
    pub project_name: String,
    pub id: i32,
}

impl From<ProjectDTO> for Project {
    fn from(project_dto: ProjectDTO) -> Self {
        Self {
            project_name: project_dto.name,
            id: project_dto.id,
        }
    }
}

impl Project {
    pub async fn get_feature_store(&self) -> Result<FeatureStore> {
        Ok(FeatureStore::from(
            feature_store::get_project_default_feature_store(self.project_name.as_str()).await?,
        ))
    }
}
