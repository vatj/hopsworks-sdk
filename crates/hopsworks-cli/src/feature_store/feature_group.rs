use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub enum FeatureGroupSubCommand {
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

pub fn mock_get_feature_group_info(name: String) {
    println!("Getting metadata information about Feature Group: {}", name);
}

pub fn mock_list_feature_groups(latest_only: bool) {
    if latest_only {
        println!("Listing latest version of each Feature Group in the current project");
    } else {
        println!("Listing all Feature Groups in the current project");
    }
}

