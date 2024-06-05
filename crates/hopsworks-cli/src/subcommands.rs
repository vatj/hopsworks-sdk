use clap::Subcommand;
use crate::platform::{ProjectSubCommand, JobSubCommand};
use crate::feature_store::{FeatureGroupSubCommand, FeatureViewSubCommand};


/// Subcommands of the Hopsworks CLI, e.g. `project` or `job`
/// These subcommands are associated to top level functionalities of
/// the Hopsworks platform and can be further expanded with more subcommands
/// and parameters.
#[derive(Debug, Subcommand)]
pub enum HopsworksCliSubCommands {
    #[command(arg_required_else_help = true)]
    Project {
        #[command(subcommand)]
        command: ProjectSubCommand,
    },
    Job {
        #[command(subcommand)]
        command: JobSubCommand,
    },
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