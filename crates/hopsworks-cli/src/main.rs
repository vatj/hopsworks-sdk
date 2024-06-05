use clap::Parser;
use color_eyre::Result;

use hopsworks_core::HopsworksClientBuilder;
use hopsworks_core::profiles::read::get_hopsworks_profile;

mod platform;
mod feature_store;
mod subcommands;

use subcommands::HopsworksCliSubCommands;
use platform::project::{self, ProjectSubCommand};
use platform::job::{self, JobSubCommand};

/// A CLI to interact with the Hopsworks Platform, Feature Store without leaving the terminal.
/// Requires a valid API key to be set in the environment variable `HOPSWORKS_API_KEY`.
#[derive(Debug, Parser)]
#[command(name = "hopsworks")]
#[command(about = "A CLI to interact with the Hopsworks Platform without leaving the terminal.", long_about = None)]
struct HopsworksCli {
    /// Subcommands of the Hopsworks CLI, e.g. `project` or `job`
    #[command(subcommand)]
    command: HopsworksCliSubCommands,
    /// Optional hopsworks profile from your local config files to use
    /// If not specified, defaults first to environment variables `HOPSWORKS-*`
    /// and then to the default_profile in the config file
    #[arg(long)]
    profile: Option<String>,
    /// Overrides the default project of the profile
    /// If specified, it will superseed the environment variable `HOPSWORKS_PROJECT_NAME`
    /// and the default project in the profile
    #[arg(long)]
    project: Option<String>,
}


#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = HopsworksCli::parse();

    let profile = get_hopsworks_profile(args.profile.as_deref())?;

    let mut hopsworks_client_builder = HopsworksClientBuilder::new_from_env_or_provided(
        &profile.user.api_key,
        &profile.cluster.get_api_url(),
        Some(profile.project.name).as_deref(),
    );
    if args.project.is_some() {
        hopsworks_client_builder =
            hopsworks_client_builder.with_project_name(args.project.unwrap().as_str());
    }

    let current_project = hopsworks_core::login(Some(hopsworks_client_builder)).await?;

    match args.command {
        HopsworksCliSubCommands::Project { command } => match command {
            ProjectSubCommand::Info {} => project::show_project_info(current_project),
            ProjectSubCommand::List {} => project::show_list_projects().await,
        },
        HopsworksCliSubCommands::Job { command } => match command {
            JobSubCommand::Info { name } => job::show_job_info(&name, current_project).await,
            JobSubCommand::List {} => job::show_list_jobs(current_project).await,
            JobSubCommand::ListExecutions { name, active } => {
                job::show_list_executions(current_project, &name, active).await
            }
            JobSubCommand::Run {
                name,
                args,
                await_termination,
                download_logs,
            } => {
                job::show_run_job(
                    current_project,
                    &name,
                    &args,
                    await_termination,
                    download_logs,
                )
                .await
            }
        },
        HopsworksCliSubCommands::FeatureGroup { command } => match command {
            feature_store::FeatureGroupSubCommand::Info { name } => {
                feature_store::feature_group::mock_get_feature_group_info(name)
            }
            feature_store::FeatureGroupSubCommand::List { latest_only } => {
                feature_store::feature_group::mock_list_feature_groups(latest_only)
            }
        },
        HopsworksCliSubCommands::FeatureView { command } => match command {
            feature_store::FeatureViewSubCommand::Info { name, version } => {
                feature_store::feature_view::mock_get_feature_view_info(name, version)
            }
            feature_store::FeatureViewSubCommand::List { latest_only } => {
                feature_store::feature_view::mock_list_feature_views(latest_only)
            }
        },
    }

    Ok(())
}
