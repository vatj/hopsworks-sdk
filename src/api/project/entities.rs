use serde::{Deserialize, Serialize};

use crate::repositories::project::entities::ProjectDTO;

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
