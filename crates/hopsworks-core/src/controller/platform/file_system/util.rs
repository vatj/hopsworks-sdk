use color_eyre::Result;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use hopsworks_internal::platform::file_system::FlowBaseParams;

const DEFAULT_FLOW_CHUNK_SIZE: usize = 1048576;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadOptions {
    pub chunk_size: usize,
    pub simultaneous_uploads: usize,
    pub max_chunk_retries: i32,
    pub chunk_retry_interval: u64,
}

impl UploadOptions {
    pub fn new(
        chunk_size: usize,
        simultaneous_uploads: usize,
        max_chunk_retries: i32,
        chunk_retry_interval: u64,
    ) -> Self {
        UploadOptions {
            chunk_size,
            simultaneous_uploads,
            max_chunk_retries,
            chunk_retry_interval,
        }
    }
}

impl Default for UploadOptions {
    fn default() -> Self {
        UploadOptions {
            chunk_size: DEFAULT_FLOW_CHUNK_SIZE,
            simultaneous_uploads: 3,
            max_chunk_retries: 3,
            chunk_retry_interval: 1000,
        }
    }
}

/// Build the path to download the file on the local fs and return to the user, it should be absolute for consistency
/// Download in CWD if local_path not specified
pub(super) async fn download_local_path_or_default(
    path: &str,
    local_path: Option<&str>,
    overwrite: bool,
) -> Result<PathBuf> {
    let local_path = match local_path {
        Some(path) => {
            if Path::new(path).is_absolute() {
                Path::new(path).to_owned()
            } else {
                let cwd = std::env::current_dir().unwrap();
                cwd.join(path)
            }
        }
        None => {
            let cwd = std::env::current_dir().unwrap();
            cwd.join(Path::new(path).file_name().unwrap())
        }
    };

    if local_path.exists() {
        if overwrite {
            if local_path.is_file() {
                tokio::fs::remove_file(&local_path).await?;
            } else {
                tokio::fs::remove_dir_all(&local_path).await?;
            }
        } else {
            return Err(color_eyre::eyre::eyre!(format!(
                "{} already exists, set overwrite=True to overwrite it",
                local_path.display()
            )));
        }
    }
    Ok(local_path.to_owned())
}

pub async fn prepare_upload(
    local_path: &str,
    upload_path: &str,
    overwrite: bool,
    chunk_size: usize,
) -> Result<(PathBuf, FlowBaseParams)> {
    let local_path = if !Path::new(local_path).is_absolute() && Path::new(local_path).exists() {
        let cwd = std::env::current_dir().unwrap();
        cwd.join(local_path)
    } else {
        Path::new(local_path).to_owned()
    };

    let file_size = tokio::fs::metadata(&local_path).await?.len() as usize;
    let file_name = local_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let destination_path = format!("{}/{}", upload_path, file_name);

    if super::file_or_dir_exists(&destination_path).await? {
        if overwrite {
            super::remove_file_or_dir(&destination_path).await?;
        } else {
            return Err(color_eyre::eyre::eyre!(format!(
                "{} already exists, set overwrite=True to overwrite it",
                destination_path
            )));
        }
    }

    let num_chunks = (file_size as f64 / chunk_size as f64).ceil() as usize;
    let params = FlowBaseParams::new(chunk_size, num_chunks, file_size, &file_name);

    Ok((local_path, params))
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_path_or_default() {
        // Test case 1: local_path is specified and is absolute
        let path = "/path/to/file.txt";
        let local_path = Some("/absolute/path/to/file.txt");
        let overwrite = false;
        let result = download_local_path_or_default(path, local_path, overwrite).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), PathBuf::from("/absolute/path/to/file.txt"));

        // Test case 2: local_path is specified and is relative
        let path = "/path/to/file.txt";
        let local_path = Some("relative/path/to/file.txt");
        let overwrite = false;
        let result = download_local_path_or_default(path, local_path, overwrite).await;
        assert!(result.is_ok());
        let cwd = std::env::current_dir().unwrap();
        assert_eq!(result.unwrap(), cwd.join("relative/path/to/file.txt"));

        // Test case 3: local_path is not specified
        let path = "/path/to/file.txt";
        let local_path = None;
        let overwrite = false;
        let result = download_local_path_or_default(path, local_path, overwrite).await;
        assert!(result.is_ok());
        let cwd = std::env::current_dir().unwrap();
        assert_eq!(result.unwrap(), cwd.join("file.txt"));

        // Test case 4: local_path is not specified and overwrite is true
        let path = "/path/to/file.txt";
        let local_path = None;
        let overwrite = true;
        let result = download_local_path_or_default(path, local_path, overwrite).await;
        assert!(result.is_ok());
        let cwd = std::env::current_dir().unwrap();
        assert_eq!(result.unwrap(), cwd.join("file.txt"));
    }
}
