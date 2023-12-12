use bytes::Bytes;
use color_eyre::Result;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::io::AsyncWriteExt;

use crate::{platform::file_system::UploadOptions, repositories::platform::file_system::service};

const DEFAULT_FLOW_CHUNK_SIZE: usize = 1048576;
const FLOW_PERMANENT_ERRORS_STATUS: [reqwest::StatusCode; 5] = [
    reqwest::StatusCode::NOT_FOUND,
    reqwest::StatusCode::PAYLOAD_TOO_LARGE,
    reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE,
    reqwest::StatusCode::INTERNAL_SERVER_ERROR,
    reqwest::StatusCode::NOT_IMPLEMENTED,
];

mod util;

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

async fn upload_chunk(
    path: &str,
    flow_params: util::FlowBaseParams,
    chunk: Bytes,
    upload_options: UploadOptions,
) -> Result<reqwest::StatusCode> {
    let retries = 0;
    let mut status = reqwest::StatusCode::PROCESSING;

    while retries < upload_options.max_chunk_retries || status != 200 {
        let resp = service::upload_request_single_chunk(
            chunk.clone(),
            path,
            flow_params.flow_filename.clone().as_str(),
            flow_params.clone(),
        )
        .await?;
        status = resp.status();

        if FLOW_PERMANENT_ERRORS_STATUS.contains(&status) {
            return Err(color_eyre::eyre::eyre!(
                "Permanent error {}, failed to upload chunk: {}",
                &status,
                resp.text().await?
            ));
        }
    }

    if retries == upload_options.max_chunk_retries {
        return Err(color_eyre::eyre::eyre!(
            "Failed to upload chunk after {} retries",
            retries
        ));
    } else {
        return Ok(status);
    }
}

pub async fn download(path: &str, local_path: Option<&str>, overwrite: bool) -> Result<String> {
    let local_path = util::local_path_or_default(path, local_path, overwrite).await?;

    let file_size = service::get_path_metadata(path)
        .await?
        .get("attributes")
        .expect("No attributes in file metadata response")
        .get("size")
        .expect("No size attribute in file metadata response")
        .as_u64()
        .expect("Invalid size attribute in file metadata response");

    let mut resp = service::download(path).await?;

    let mut file = tokio::fs::File::create(local_path.clone()).await?;
    let pbar = ProgressBar::new(file_size as u64);
    pbar.set_style(
            ProgressStyle::default_bar()
                .template("{desc}: {percentage:.3}%|{bar}| {bytes}/{total_bytes} elapsed<{elapsed} remaining<{eta}")?
                .progress_chars("#>-"),
        );

    while let Some(chunk) = resp.chunk().await? {
        file.write_all(&chunk).await?;
        pbar.inc(chunk.len() as u64);
    }

    pbar.finish();
    Ok(local_path
        .to_str()
        .expect("Local path is not valid UTF-8")
        .to_string())
}
