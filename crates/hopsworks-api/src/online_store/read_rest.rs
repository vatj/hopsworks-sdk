use color_eyre::Result;

use hopsworks_core::controller::platform::variables::get_loadbalancer_external_domain;

async fn init_online_store_rest_client(api_key: Option<&str>, api_version: Option<&str>, reqwest_client: Option<reqwest::client>) -> Result<()> {
    let url = get_loadbalancer_external_domain(("online_store_rest_server")).await?;
    todo!()
}