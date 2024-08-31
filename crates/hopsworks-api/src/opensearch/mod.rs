use color_eyre::Result;
use hopsworks_core::controller::platform::{
    opensearch::get_opensearch_auth_token, variables::get_loadbalancer_external_domain,
};

use hopsworks_opensearch;

#[tracing::instrument(fields(opensearch_url))]
pub async fn init_hopsworks_opensearch_client(project_id: i32) -> Result<()> {
    let opensearch_host = get_loadbalancer_external_domain("opensearch").await?;
    let opensearch_url = format!("https://{}:9092", opensearch_host);
    let token = get_opensearch_auth_token(project_id).await?;
    hopsworks_opensearch::init_hopsworks_opensearch_client(&opensearch_url, &token)?;
    Ok(())
}

#[cfg(feature = "blocking")]
pub fn init_hopsworks_opensearch_client_blocking(
    project_id: i32,
    multithreaded: bool,
) -> Result<()> {
    let rt = hopsworks_core::get_hopsworks_runtime(multithreaded);
    let _guard = rt.enter();

    rt.block_on(init_hopsworks_opensearch_client(project_id))
}
