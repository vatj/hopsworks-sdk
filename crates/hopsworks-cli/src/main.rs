use clap::{Parser, Subcommand};
use hopsworks_utils::get_hopsworks_profiles;
use log::debug;

/// A CLI to interact with the Hopsworks Platform without leaving the terminal.
/// Requires a valid API key to be set in the environment variable `HOPSWORKS_API_KEY`.
#[derive(Debug, Parser)]
#[command(name = "hopsworks")]
#[command(about = "A CLI to interact with the Hopsworks Platform without leaving the terminal.", long_about = None)]
struct HopsworksCli {
    #[command(subcommand)]
    command: HopsworksCliSubCommands,
}

#[derive(Debug, Subcommand)]
enum HopsworksCliSubCommands {
    #[command(arg_required_else_help = true)]
    Project {
        #[command(subcommand)]
        command: ProjectSubCommand,
    },
    Job {
        #[command(subcommand)]
        command: JobSubCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ProjectSubCommand {
    /// Get metadata information about a project, defaults to current project
    Info {
        /// Optional id
        id: Option<u64>,
    },
    /// List all projects associated to the set API key
    List {},
}

#[derive(Debug, Subcommand)]
#[command(flatten_help = true)]
enum JobSubCommand {
    /// Get metadata information about a job
    #[command(arg_required_else_help = true)]
    Info {
        /// Job name in the current project or the project must
        /// be specified with the `--project` flag
        #[arg(short, long, required = true)]
        name: String,
        /// Optional project name
        /// If not specified, the current project is used
        #[arg(long)]
        project: Option<String>,
    },
    /// List all jobs in the current project or the project must
    /// be specified with the `--project` flag
    List {
        /// Optional project name
        /// If not specified, the current project is used
        #[arg(long)]
        project: Option<String>,
    },
}

fn mock_get_project_info(id: Option<u64>) {
    println!("Getting project info with id: {:?}", id);
}

fn mock_list_projects() {
    println!("Listing all projects");
}

fn mock_get_job_info(name: String, project: Option<String>) {
    println!(
        "Getting job info with name: {:?} and project: {:?}",
        name, project
    );
}

fn mock_list_jobs(project: Option<String>) {
    println!("Listing all jobs with project: {:?}", project);
}

fn main() {
    env_logger::init();

    let args = HopsworksCli::parse();

    let config_file = get_hopsworks_profiles().unwrap();
    debug!("Using config file: {:?}", config_file);

    match args.command {
        HopsworksCliSubCommands::Project { command } => match command {
            ProjectSubCommand::Info { id } => mock_get_project_info(id),
            ProjectSubCommand::List {} => mock_list_projects(),
        },
        HopsworksCliSubCommands::Job { command } => match command {
            JobSubCommand::Info { name, project } => mock_get_job_info(name, project),
            JobSubCommand::List { project } => mock_list_jobs(project),
        },
    }
}
