use color_eyre::Result;

use super::ProjectDTO;
use super::payloads::NewProjectPayload;
use super::service::get_project_and_user_list;

pub async fn get_project_list() -> Result<Vec<ProjectDTO>> {
    Ok(get_project_and_user_list()
        .await?
        .iter()
        .map(|project_and_user| project_and_user.project.to_owned())
        .collect())
}

pub async fn create_project(project_name: &str, description: &Option<&str>) -> Result<ProjectDTO> {
    let new_project_payload = NewProjectPayload::new(project_name, description);
    let mut project_and_user_list =
        super::service::create_project(&new_project_payload)
            .await?;

    Ok(project_and_user_list
            .pop()
            .expect("ProjectAndUserDTO list should not be empty")
            .project)
}
