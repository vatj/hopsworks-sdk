use color_eyre::Result;
use hopsworks_core::controller::platform::{variables::get_loadbalancer_external_domain, opensearch::get_opensearch_auth_token};

use hopsworks_opensearch;

pub async fn init_hopsworks_opensearch_client(project_id: i32) -> Result<()> {
    let opensearch_url = get_loadbalancer_external_domain("opensearch").await?;
    let token = get_opensearch_auth_token(project_id).await?;
    hopsworks_opensearch::init_hopsworks_opensearch_client(&opensearch_url, &token)?;
    Ok(())
}

#[cfg(feature = "blocking")]
pub fn init_hopsworks_opensearch_client_blocking(project_id: i32, multithreaded: bool) -> Result<()> {
    let rt = hopsworks_core::get_hopsworks_runtime(multithreaded);
    let _guard = rt.enter();

    rt.block_on(init_hopsworks_opensearch_client(project_id))
}