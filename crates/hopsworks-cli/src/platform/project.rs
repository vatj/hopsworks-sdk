use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub enum ProjectSubCommand {
    /// Get metadata information about the current project
    Info {},
    /// List all projects associated to the set API key
    List {},
}

pub fn show_project_info(current_project: hopsworks_core::platform::project::Project) {
    println!("Current project: {:#?}", current_project);
}

pub async fn show_list_projects() {
    let projects = hopsworks_core::platform::project::get_project_list().await;
    println!("Listing all projects: {:#?}", projects)
}
