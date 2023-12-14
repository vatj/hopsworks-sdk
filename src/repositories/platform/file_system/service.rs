use color_eyre::Result;
use reqwest::Method;

use crate::{core::platform::file_system::util::FlowBaseParams, get_hopsworks_client};

pub async fn remove(path: &str) -> Result<()> {
    let resp = get_hopsworks_client()
        .await
        .request(
            Method::DELETE,
            format!("dataset/{}", path).as_str(),
            true,
            true,
        )
        .await?
        .send()
        .await?;

    match resp.status() {
        reqwest::StatusCode::OK => Ok(()),
        _ => Err(color_eyre::eyre::eyre!(
            "Failed to remove dataset: {}",
            resp.text().await?
        )),
    }
}

pub async fn get_path_metadata(path: &str) -> Result<serde_json::Value> {
    Ok(get_hopsworks_client()
        .await
        .request(
            Method::GET,
            format!("dataset/{}", path).as_str(),
            true,
            true,
        )
        .await?
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?)
}

pub async fn mkdir(path: &str) -> Result<()> {
    let resp = get_hopsworks_client()
        .await
        .request(
            Method::POST,
            format!("dataset/{}", path).as_str(),
            true,
            true,
        )
        .await?
        .query(&[
            ("action", "create"),
            ("searchable", "true"),
            ("generate_readme", "false"),
            ("type", "DATASET"),
        ])
        .send()
        .await?;

    match resp.status() {
        reqwest::StatusCode::OK => Ok(()),
        _ => Err(color_eyre::eyre::eyre!(
            "Failed to create directory: {}",
            resp.text().await?
        )),
    }
}

pub async fn move_file_or_dir(src_path: &str, dst_path: &str) -> Result<()> {
    let resp = get_hopsworks_client()
        .await
        .request(
            Method::POST,
            format!("dataset/{}", src_path).as_str(),
            true,
            true,
        )
        .await?
        .query(&[("action", "move"), ("destination", dst_path)])
        .send()
        .await?;

    match resp.status() {
        reqwest::StatusCode::OK => Ok(()),
        _ => Err(color_eyre::eyre::eyre!(
            "Failed to move dataset: {}",
            resp.text().await?
        )),
    }
}

pub async fn copy(src_path: &str, dst_path: &str) -> Result<()> {
    let resp = get_hopsworks_client()
        .await
        .request(
            Method::POST,
            format!("dataset/{}", src_path).as_str(),
            true,
            true,
        )
        .await?
        .query(&[("action", "copy"), ("destination", dst_path)])
        .send()
        .await?;

    match resp.status() {
        reqwest::StatusCode::OK => Ok(()),
        _ => Err(color_eyre::eyre::eyre!(
            "Failed to copy dataset: {}",
            resp.text().await?
        )),
    }
}

pub async fn upload_request_single_chunk(
    chunk: Vec<u8>,
    path: &str,
    form: &str,
    flow_params: FlowBaseParams,
) -> Result<reqwest::Response> {
    let resp = get_hopsworks_client()
        .await
        .request(
            Method::POST,
            format!("dataset/upload/{}", path).as_str(),
            true,
            true,
        )
        .await?
        .query(&flow_params.to_query_params())
        .form(form)
        .body(chunk)
        .send()
        .await?;

    Ok(resp)
}

pub async fn download(path: &str) -> Result<reqwest::Response> {
    Ok(get_hopsworks_client()
        .await
        .request(
            Method::GET,
            format!("dataset/download/with_auth/{}", path).as_str(),
            true,
            true,
        )
        .await?
        .query(&[("type", "Dataset")])
        .send()
        .await?)
}
