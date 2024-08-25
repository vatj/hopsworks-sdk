pub mod controller;
pub mod entities;
mod client;
mod rondb_feature_store_api;
mod payload;

use color_eyre::Result;
use reqwest::header::HeaderValue;
use std::sync::OnceLock;

// Can be upgraded to a lookup table if we want to have a per Feature View client
static ONLINE_STORE_REST_CLIENT : OnceLock<client::OnlineStoreRestClient> = OnceLock::new();

fn get_online_store_rest_client() -> Result<&'static client::OnlineStoreRestClient> {
    match ONLINE_STORE_REST_CLIENT.get() {
        Some(the_client) => Ok(the_client),
        None => color_eyre::eyre::bail!("Online Store Rest Client not initialized. Call init_online_store_rest_client() first.")
    }
}

pub fn init_online_store_rest_client(url: &str, api_key: HeaderValue, api_version: &str, reqwest_client: Option<reqwest::Client>) -> Result<()> {
    let builder = client::OnlineStoreRestClient::builder()
        .api_key(api_key)
        .url(url.to_string())
        .api_version(api_version.to_string())
        .client(reqwest_client.unwrap_or_default());

    ONLINE_STORE_REST_CLIENT.get_or_init(|| builder.build());
    Ok(())
}


