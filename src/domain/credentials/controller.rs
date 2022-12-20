use color_eyre::Result;
use log::debug;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::repositories::credentials::service::get_hopsworks_credentials_for_project;

pub async fn write_locally_project_credentials_on_login(
    project_id: i32,
    project_name: &str,
    cert_dir: &str,
) -> Result<()> {
    let credentials_dto = get_hopsworks_credentials_for_project(project_id).await?;

    if !Path::new(cert_dir).join(project_name).exists() {
        tokio::fs::create_dir(Path::new(cert_dir).join(project_name)).await?;
    }

    write_cert_to_file(
        project_name,
        "ca_chain.pem",
        cert_dir,
        &credentials_dto.ca_chain,
    )
    .await?;

    write_cert_to_file(
        project_name,
        "client_key.pem",
        cert_dir,
        &credentials_dto.client_key,
    )
    .await?;

    write_cert_to_file(
        project_name,
        "client_cert.pem",
        cert_dir,
        &credentials_dto.client_cert,
    )
    .await?;

    write_cert_to_file(
        project_name,
        "key_store.jks",
        cert_dir,
        &credentials_dto.k_store,
    )
    .await?;

    write_cert_to_file(
        project_name,
        "trust_store.jks",
        cert_dir,
        &credentials_dto.t_store,
    )
    .await?;

    Ok(())
}

async fn write_cert_to_file(
    project_name: &str,
    cert_file_name: &str,
    cert_dir: &str,
    cert_text: &str,
) -> Result<()> {
    let cert_file_path = Path::new(cert_dir).join(project_name).join(cert_file_name);
    if !cert_file_path.exists() {
        debug!(
            "Writing {} to {:?}",
            cert_file_name,
            cert_file_path.as_path()
        );
        let mut cert_file = File::create(cert_file_path).await?;
        cert_file.write_all(cert_text.as_bytes()).await?;
    }

    Ok(())
}
