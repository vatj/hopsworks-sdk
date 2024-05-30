use clap::{Parser, Subcommand};

/// A CLI to interact with the Hopsworks Feature Store without leaving the terminal.
/// Requires a valid API key to be set in the environment variable `HOPSWORKS_API_KEY`.
#[derive(Debug, Parser)]
#[command(name = "hopsworks-fs")]
#[command(about = "A CLI to interact with the Hopsworks Feature Store without leaving the terminal.", long_about = None)]
struct HopsworksFeatureStoreCli {
    #[command(subcommand)]
    command: HopsworksFeatureStoreCliSubCommands,
}

#[derive(Debug, Subcommand)]
enum HopsworksFeatureStoreCliSubCommands {
    #[command(arg_required_else_help = true)]
    FeatureGroup {
        #[command(subcommand)]
        command: FeatureGroupSubCommand,
    },
    FeatureView {
        #[command(subcommand)]
        command: FeatureViewSubCommand,
    },
}

#[derive(Debug, Subcommand)]
enum FeatureGroupSubCommand {
    /// Get metadata information about a Feature Group, defaults to current project
    #[command(arg_required_else_help = true)]
    Info {
        /// Feature Group name in the current project
        #[arg(short, long, required = true)]
        name: String,
    },
    /// List all Feature Groups in the current project
    List {
        /// Only list latest version of each Feature Group
        #[arg(long, default_missing_value = "true")]
        latest_only: bool,
    },
}

#[derive(Debug, Subcommand)]
#[command(flatten_help = true)]
enum FeatureViewSubCommand {
    /// Get metadata information about a Feature View in the current project
    #[command(arg_required_else_help = true)]
    Info {
        /// Feature View name in the current project
        #[arg(short, long, required = true)]
        name: String,
        /// Optional version of the Feature View
        /// If not specified, the latest version is used
        #[arg(short, long)]
        version: Option<i32>,
    },
    /// List all Feature Views in the current project
    List {
        /// Only list latest version of each Feature View
        #[arg(long, default_missing_value = "true")]
        latest_only: bool,
    },
}

fn mock_get_feature_group_info(name: String) {
    println!("Getting metadata information about Feature Group: {}", name);
}

fn mock_list_feature_groups(latest_only: bool) {
    if latest_only {
        println!("Listing latest version of each Feature Group in the current project");
    } else {
        println!("Listing all Feature Groups in the current project");
    }
}

fn mock_get_feature_view_info(name: String, version: Option<i32>) {
    match version {
        Some(v) => println!(
            "Getting metadata information about Feature View: {} with version: {}",
            name, v
        ),
        None => println!(
            "Getting metadata information about Feature View: {} with the latest version",
            name
        ),
    }
}

fn mock_list_feature_views(latest_only: bool) {
    if latest_only {
        println!("Listing latest version of each Feature View in the current project");
    } else {
        println!("Listing all Feature Views in the current project");
    }
}

fn main() {
    let args = HopsworksFeatureStoreCli::parse();

    match args.command {
        HopsworksFeatureStoreCliSubCommands::FeatureGroup { command } => match command {
            FeatureGroupSubCommand::Info { name } => mock_get_feature_group_info(name),
            FeatureGroupSubCommand::List { latest_only } => mock_list_feature_groups(latest_only),
        },
        HopsworksFeatureStoreCliSubCommands::FeatureView { command } => match command {
            FeatureViewSubCommand::Info { name, version } => {
                mock_get_feature_view_info(name, version)
            }
            FeatureViewSubCommand::List { latest_only } => mock_list_feature_views(latest_only),
        },
    }
}
