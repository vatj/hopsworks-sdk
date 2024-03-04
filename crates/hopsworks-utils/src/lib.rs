use color_eyre::eyre::Result;
use directories::BaseDirs;
use log::debug;

pub mod hopsworks_configs;

pub fn get_hopsworks_profiles_config_file() -> Result<std::path::PathBuf> {
    if let Some(base_dirs) = BaseDirs::new() {
        let config_dir = base_dirs.config_dir();
        if config_dir.exists() {
            debug!("Found existing config directory: {:?}", config_dir);
        } else {
            debug!("Creating config directory: {:?}", config_dir);
            std::fs::create_dir_all(config_dir)?;
            debug!("Created config directory: {:?}", config_dir);
        }

        let config_file = config_dir.join("hopsworks-profiles.toml");
        if config_file.exists() {
            debug!("Found existing config file: {:?}", config_file);
        } else {
            debug!("Creating config file: {:?}", config_file);
            std::fs::write(&config_file, "")?;
            debug!("Created config file: {:?}", config_file);
        }

        Ok(config_file)
    } else {
        Err(color_eyre::eyre::eyre!("Failed to get config directory exists, check that directories::BaseDirs::new() is supported on this platform."))
    }
}
