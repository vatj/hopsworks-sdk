use clap::Subcommand;

#[derive(Debug, Subcommand)]
#[command(flatten_help = true)]
pub enum FeatureViewSubCommand {
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

pub fn mock_get_feature_view_info(name: String, version: Option<i32>) {
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

pub fn mock_list_feature_views(latest_only: bool) {
    if latest_only {
        println!("Listing latest version of each Feature View in the current project");
    } else {
        println!("Listing all Feature Views in the current project");
    }
}