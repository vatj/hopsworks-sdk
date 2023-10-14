use color_eyre::Result;
use log::debug;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::repositories::credentials::service::get_hopsworks_credentials_for_project;

pub async fn write_locally_project_credentials_on_login(cert_dir: &str) -> Result<String> {
    let credentials_dto = get_hopsworks_credentials_for_project().await?;

    if !Path::new(cert_dir).exists() {
        debug!("Creating cert dir: {:?}", cert_dir);
        tokio::fs::create_dir(Path::new(cert_dir)).await?;
    }

    write_cert_to_file("ca_chain.pem", cert_dir, &credentials_dto.ca_chain).await?;

    write_cert_to_file("client_key.pem", cert_dir, &credentials_dto.client_key).await?;

    write_cert_to_file("client_cert.pem", cert_dir, &credentials_dto.client_cert).await?;

    write_cert_to_file("key_store.jks", cert_dir, &credentials_dto.k_store).await?;

    write_cert_to_file("trust_store.jks", cert_dir, &credentials_dto.t_store).await?;

    write_cert_to_file("material_passwd", cert_dir, &credentials_dto.password).await?;

    Ok(credentials_dto.password.clone())
}

async fn write_cert_to_file(cert_file_name: &str, cert_dir: &str, cert_text: &str) -> Result<()> {
    let cert_file_path = Path::new(cert_dir).join(cert_file_name);
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
