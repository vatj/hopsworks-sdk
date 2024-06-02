//! Interact with your Project's File System
use color_eyre::Result;
use serde::{Deserialize, Serialize};

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

pub async fn upload_to_hopsworks(
    local_path: &str,
    upload_path: &str,
    overwrite: bool,
    upload_options: Option<UploadOptions>,
) -> Result<String> {
    crate::controller::platform::file_system::upload(
        local_path,
        upload_path,
        overwrite,
        upload_options.unwrap_or_default(),
    )
    .await
}

pub async fn download_from_hopsworks(
    remote_path: &str,
    local_path: Option<&str>,
    overwrite: bool,
) -> Result<String> {
    crate::controller::platform::file_system::download(remote_path, local_path, overwrite).await
}

pub async fn remove_from_hopsworks(path: &str) -> Result<()> {
    crate::controller::platform::file_system::remove_file_or_dir(path).await
}

pub async fn mkdir_in_hopsworks(path: &str) -> Result<()> {
    crate::controller::platform::file_system::mkdir(path).await
}

pub async fn move_file_or_dir_in_hopsworks(
    src_path: &str,
    dst_path: &str,
    overwrite: bool,
) -> Result<()> {
    crate::controller::platform::file_system::move_file_or_dir(src_path, dst_path, overwrite).await
}

pub async fn copy_file_or_dir_in_hopsworks(
    src_path: &str,
    dst_path: &str,
    overwrite: bool,
) -> Result<()> {
    crate::controller::platform::file_system::copy(src_path, dst_path, overwrite).await
}

pub async fn file_or_dir_exists_in_hopsworks(path: &str) -> Result<bool> {
    crate::controller::platform::file_system::file_or_dir_exists(path).await
}
