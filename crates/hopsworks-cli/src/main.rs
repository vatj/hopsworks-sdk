use clap::{Parser, Subcommand};
use color_eyre::Result;

use hopsworks::HopsworksClientBuilder;
use hopsworks_base::profiles::read::get_hopsworks_profile;

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

// /// A CLI to interact with the Hopsworks Feature Store without leaving the terminal.
// /// Requires a valid API key to be set in the environment variable `HOPSWORKS_API_KEY`.
// #[derive(Debug, Parser)]
// #[command(name = "hopsworks-fs")]
// #[command(about = "A CLI to interact with the Hopsworks Feature Store without leaving the terminal.", long_about = None)]
// struct HopsworksFeatureStoreCli {
//     #[command(subcommand)]
//     command: HopsworksFeatureStoreCliSubCommands,
// }

// #[derive(Debug, Subcommand)]
// enum HopsworksFeatureStoreCliSubCommands {
//     #[command(arg_required_else_help = true)]
//     FeatureGroup {
//         #[command(subcommand)]
//         command: FeatureGroupSubCommand,
//     },
//     FeatureView {
//         #[command(subcommand)]
//         command: FeatureViewSubCommand,
//     },
// }

// #[derive(Debug, Subcommand)]
// enum FeatureGroupSubCommand {
//     /// Get metadata information about a Feature Group, defaults to current project
//     #[command(arg_required_else_help = true)]
//     Info {
//         /// Feature Group name in the current project
//         #[arg(short, long, required = true)]
//         name: String,
//     },
//     /// List all Feature Groups in the current project
//     List {
//         /// Only list latest version of each Feature Group
//         #[arg(long, default_missing_value = "true")]
//         latest_only: bool,
//     },
// }

// #[derive(Debug, Subcommand)]
// #[command(flatten_help = true)]
// enum FeatureViewSubCommand {
//     /// Get metadata information about a Feature View in the current project
//     #[command(arg_required_else_help = true)]
//     Info {
//         /// Feature View name in the current project
//         #[arg(short, long, required = true)]
//         name: String,
//         /// Optional version of the Feature View
//         /// If not specified, the latest version is used
//         #[arg(short, long)]
//         version: Option<i32>,
//     },
//     /// List all Feature Views in the current project
//     List {
//         /// Only list latest version of each Feature View
//         #[arg(long, default_missing_value = "true")]
//         latest_only: bool,
//     },
// }

// fn mock_get_feature_group_info(name: String) {
//     println!("Getting metadata information about Feature Group: {}", name);
// }

// fn mock_list_feature_groups(latest_only: bool) {
//     if latest_only {
//         println!("Listing latest version of each Feature Group in the current project");
//     } else {
//         println!("Listing all Feature Groups in the current project");
//     }
// }

// fn mock_get_feature_view_info(name: String, version: Option<i32>) {
//     match version {
//         Some(v) => println!(
//             "Getting metadata information about Feature View: {} with version: {}",
//             name, v
//         ),
//         None => println!(
//             "Getting metadata information about Feature View: {} with the latest version",
//             name
//         ),
//     }
// }

// fn mock_list_feature_views(latest_only: bool) {
//     if latest_only {
//         println!("Listing latest version of each Feature View in the current project");
//     } else {
//         println!("Listing all Feature Views in the current project");
//     }
// }

// fn main() {
//     let args = HopsworksFeatureStoreCli::parse();

//     match args.command {
//         HopsworksFeatureStoreCliSubCommands::FeatureGroup { command } => match command {
//             FeatureGroupSubCommand::Info { name } => mock_get_feature_group_info(name),
//             FeatureGroupSubCommand::List { latest_only } => mock_list_feature_groups(latest_only),
//         },
//         HopsworksFeatureStoreCliSubCommands::FeatureView { command } => match command {
//             FeatureViewSubCommand::Info { name, version } => {
//                 mock_get_feature_view_info(name, version)
//             }
//             FeatureViewSubCommand::List { latest_only } => mock_list_feature_views(latest_only),
//         },
//     }
// }

