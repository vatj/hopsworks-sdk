use std::path::{Path, PathBuf};

use bytes::Bytes;
use color_eyre::Result;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::{platform::file_system::UploadOptions, repositories::platform::file_system::service};

pub async fn file_or_dir_exists(path: &str) -> Result<bool> {
    let resp = service::get_path_metadata(path).await;

    match resp {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}

pub async fn remove_file_or_dir(path: &str) -> Result<()> {
    service::remove(path).await
}

pub async fn move_file_or_dir(src_path: &str, dst_path: &str, overwrite: bool) -> Result<()> {
    let dst_exists = file_or_dir_exists(dst_path).await?;

    if overwrite && dst_exists {
        remove_file_or_dir(dst_path).await?;
    } else if !overwrite && dst_exists {
        return Err(color_eyre::eyre::eyre!(
            "Destination path {} already exists, set overwrite=Some(true) to overwrite it",
            dst_path,
        ));
    }

    service::move_file_or_dir(src_path, dst_path).await?;

    Ok(())
}

pub async fn copy(src_path: &str, dst_path: &str, overwrite: bool) -> Result<()> {
    let dst_exists = file_or_dir_exists(dst_path).await?;

    if overwrite && dst_exists {
        remove_file_or_dir(dst_path).await?;
    } else if !overwrite && dst_exists {
        return Err(color_eyre::eyre::eyre!(
            "Destination path {} already exists, set overwrite=Some(true) to overwrite it",
            dst_path,
        ));
    }

    service::copy(src_path, dst_path).await?;

    Ok(())
}

pub async fn upload_file(
    local_path: &str,
    upload_path: &str,
    overwrite: bool,
    upload_options: Option<UploadOptions>,
) -> Result<String> {
    todo!()
}

async fn upload_chunk(flow_params: FlowBaseParams, chunk: Bytes) -> Result<String> {
    todo!()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowBaseParams {
    template_id: i32,
    flow_chunk_size: usize,
    flow_total_size: usize,
    flow_identifier: String,
    flow_filename: String,
    flow_relative_path: String,
    flow_total_chunks: usize,
    flow_current_chunk_size: usize,
    flow_chunk_number: usize,
}

impl FlowBaseParams {
    pub fn new(chunk_size: usize, num_chunks: usize, total_size: usize, file_name: &str) -> Self {
        Self {
            template_id: -1,
            flow_chunk_size: chunk_size,
            flow_total_size: total_size,
            flow_identifier: format!("{}_{}", total_size, file_name),
            flow_filename: file_name.to_string(),
            flow_relative_path: file_name.to_string(),
            flow_total_chunks: num_chunks,
            flow_current_chunk_size: 0,
            flow_chunk_number: 0,
        }
    }
}

pub async fn download(path: &str, local_path: Option<&str>, overwrite: bool) -> Result<String> {
    let local_path = local_path_or_default(path, local_path, overwrite).await?;

    let file_size = service::get_path_metadata(path)?
        .attributes
        .size
        .parse::<usize>()
        .unwrap();
    let resp = service::download(path, &local_path, file_size).await?;

    let mut file = tokio::fs::File::create(local_path).await?;
    let mut pbar = ProgressBar::new(file_size as u64);
    pbar.set_style(
            ProgressStyle::default_bar()
                .template("{desc}: {percentage:.3}%|{bar}| {bytes}/{total_bytes} elapsed<{elapsed} remaining<{eta}")?
                .progress_chars("#>-"),
        );

    while let Some(chunk) = response.chunk() {
        file.write_all(&chunk?).await?;
        pbar.inc(chunk?.len() as u64);
    }

    pbar.finish();
    Ok(local_path.to_string_lossy().to_string())
}

/// Build the path to download the file on the local fs and return to the user, it should be absolute for consistency
/// Download in CWD if local_path not specified
async fn local_path_or_default(
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
