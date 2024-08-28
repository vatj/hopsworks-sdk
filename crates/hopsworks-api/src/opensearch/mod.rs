use color_eyre::Result;
use hopsworks_core::controller::platform::variables::get_loadbalancer_external_domain;
use hopsworks_opensearch;

pub async fn init_hopsworks_opensearch_client() -> Result<()> {
    let opensearch_url = get_loadbalancer_external_domain("opensearch").await?;
    hopsworks_opensearch::init_hopsworks_opensearch_client(&opensearch_url)?;
    Ok(())
}

#[cfg(feature = "blocking")]
pub fn init_hopsworks_opensearch_client_blocking(multithreaded: bool) -> Result<()> {
    let rt = hopsworks_core::get_hopsworks_runtime(multithreaded);
    let _guard = rt.enter();

    rt.block_on(init_hopsworks_opensearch_client())
}