use super::entities::Project;
use crate::repositories::project::entities::ProjectDTO;

impl From<ProjectDTO> for Project {
    fn from(project_dto: ProjectDTO) -> Self {
        Self {
            project_name: project_dto.name,
            id: project_dto.id,
        }
    }
}
