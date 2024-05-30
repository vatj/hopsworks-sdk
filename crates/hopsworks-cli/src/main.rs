use clap::{Parser, Subcommand};
use color_eyre::Result;

use hopsworks::HopsworksClientBuilder;
use hopsworks_utils::get_hopsworks_profile;

mod platform;

/// A CLI to interact with the Hopsworks Platform without leaving the terminal.
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

/// Subcommands of the Hopsworks CLI, e.g. `project` or `job`
/// These subcommands are associated to top level functionalities of
/// the Hopsworks platform and can be further expanded with more subcommands
/// and parameters.
#[derive(Debug, Subcommand)]
enum HopsworksCliSubCommands {
    #[command(arg_required_else_help = true)]
    Project {
        #[command(subcommand)]
        command: project::ProjectSubCommand,
    },
    Job {
        #[command(subcommand)]
        command: job::JobSubCommand,
    },
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

    let current_project = hopsworks::login(Some(hopsworks_client_builder)).await?;

    match args.command {
        HopsworksCliSubCommands::Project { command } => match command {
            project::ProjectSubCommand::Info {} => project::show_project_info(current_project),
            project::ProjectSubCommand::List {} => project::show_list_projects().await,
        },
        HopsworksCliSubCommands::Job { command } => match command {
            job::JobSubCommand::Info { name } => job::show_job_info(&name, current_project).await,
            job::JobSubCommand::List {} => job::show_list_jobs(current_project).await,
            job::JobSubCommand::ListExecutions { name, active } => {
                job::show_list_executions(current_project, &name, active).await
            }
            job::JobSubCommand::Run {
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
    }

    Ok(())
}
