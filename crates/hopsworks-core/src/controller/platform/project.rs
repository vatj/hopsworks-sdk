use color_eyre::Result;

use hopsworks_internal::platform::project::{payloads::NewProjectPayload, service};
use crate::Project;

pub async fn get_project_list() -> Result<Vec<Project>> {
    Ok(service::get_project_and_user_list()
        .await?
        .iter()
        .map(|project_and_user| project_and_user.project.to_owned())
        .collect())
}

pub async fn create_project(project_name: &str, description: &Option<&str>) -> Result<Project> {
    let new_project_payload = NewProjectPayload::new(project_name, description);
    let mut project_and_user_list =
        service::create_project(&new_project_payload)
            .await?;

    Ok(project_and_user_list
            .pop()
            .expect("ProjectAndUserDTO list should not be empty")
            .project)
}
