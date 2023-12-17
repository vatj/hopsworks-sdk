use color_eyre::Result;
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{platform::file_system::UploadOptions, repositories::platform::file_system::service};

const FLOW_PERMANENT_ERRORS_STATUS: [reqwest::StatusCode; 5] = [
    reqwest::StatusCode::NOT_FOUND,
    reqwest::StatusCode::PAYLOAD_TOO_LARGE,
    reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE,
    reqwest::StatusCode::INTERNAL_SERVER_ERROR,
    reqwest::StatusCode::NOT_IMPLEMENTED,
];

pub mod util;

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

pub async fn upload(
    local_path: &str,
    upload_path: &str,
    overwrite: bool,
    upload_options: UploadOptions,
) -> Result<String> {
    let (local_path, base_params) = util::prepare_upload(
        local_path,
        upload_path,
        overwrite,
        upload_options.chunk_size,
    )
    .await?;

    let mut chunk_number = 1;
    let mut file = tokio::fs::File::open(&local_path).await?;
    let pbar = Arc::new(ProgressBar::new(base_params.flow_total_size as u64));
    let upload_path = Arc::new(upload_path.to_owned());
    let chunk_size = upload_options.chunk_size;
    let max_chunk_retries = upload_options.max_chunk_retries;
    let chunk_retry_interval = upload_options.chunk_retry_interval;
    pbar.set_style(
            ProgressStyle::default_bar()
                .template("{desc}: {percentage:.3}%|{bar}| {bytes}/{total_bytes} elapsed<{elapsed} remaining<{eta}")?
                .progress_chars("#>-"),
        );

    let mut handles = vec![];
    loop {
        chunk_number += 1;
        let mut chunk: Vec<u8> = Vec::with_capacity(chunk_size);
        let bytes_read = file.read_buf(&mut chunk).await?;
        if bytes_read == 0 {
            break;
        }
        let base_params = base_params.clone();
        let upload_path = upload_path.clone();
        let pbar = pbar.clone();

        let handle = tokio::spawn(async move {
            let chunk_len = chunk.len() as u64;
            let status = upload_chunk(
                &upload_path,
                base_params,
                chunk,
                max_chunk_retries,
                chunk_retry_interval,
            )
            .await;
            match status {
                Ok(_) => {
                    pbar.inc(chunk_len);
                }
                Err(_) => {
                    pbar.println(format!("Failed to upload chunk {}", chunk_number));
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await?;
    }

    pbar.finish();
    Ok(format!("{}/{}", upload_path, base_params.flow_filename))
}

async fn upload_chunk(
    path: &str,
    flow_params: util::FlowBaseParams,
    chunk: Vec<u8>,
    max_chunk_retries: i32,
    chunk_retry_interval: u64,
) -> Result<reqwest::StatusCode> {
    let retries = 0;

    loop {
        match service::upload_request_single_chunk(
            chunk.clone(),
            path,
            flow_params.flow_filename.clone().as_str(),
            flow_params.clone(),
        )
        .await
        {
            Ok(resp) => {
                if FLOW_PERMANENT_ERRORS_STATUS.contains(&resp.status()) {
                    return Err(color_eyre::eyre::eyre!(
                        "Permanent error {}, failed to upload chunk: {}",
                        &resp.status(),
                        resp.text().await?
                    ));
                }
                return Ok(resp.status());
            }
            Err(_) => {
                if retries == max_chunk_retries {
                    return Err(color_eyre::eyre::eyre!(
                        "Failed to upload chunk after {} retries",
                        retries
                    ));
                } else {
                    tokio::time::sleep(tokio::time::Duration::from_millis(chunk_retry_interval))
                        .await;
                }
            }
        }
    }
}

pub async fn download(path: &str, local_path: Option<&str>, overwrite: bool) -> Result<String> {
    let local_path = util::download_local_path_or_default(path, local_path, overwrite).await?;

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
