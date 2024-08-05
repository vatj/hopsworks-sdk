use std::sync::OnceLock;
use color_eyre::Result;
use rondb_feature_store_api::ping_rondb_rest_server;
use tracing::debug;

mod client;
mod rondb_feature_store_api;
mod entities;
mod payload;

use client::OnlineStoreRestClient;

static DEFAULT_ONLINE_STORE_PORT: u16 = 5005;
static ONLINE_STORE_REST_CLIENT: OnceLock<OnlineStoreRestClient> = OnceLock::new();

pub async fn get_rondb_rest_client() -> &'static OnlineStoreRestClient {
    debug!("Access global RonDB REST Client");
    match ONLINE_STORE_REST_CLIENT.get() {
        Some(client) => client,
        None => panic!(
            "First use hopsworks_online_store::inint_rondb_rest_client() to initialize the RonDB REST client with your credentials."
        ),
    }
}

#[tracing::instrument(skip(api_key))]
pub async fn init_rondb_rest_client(api_key: &str,  hostname: &str, api_version: &str, port: Option<u16>, reqwest_client: Option<reqwest::Client>) -> Result<()> {
    let inner_client = reqwest_client.unwrap_or_default();
    ONLINE_STORE_REST_CLIENT.get_or_init(|| { 
        OnlineStoreRestClient::builder()
            .api_key(api_key.to_string())
            .api_version(api_version.to_string())
            .hostname(hostname.to_string())
            .port(port.unwrap_or(DEFAULT_ONLINE_STORE_PORT))
            .inner_client(inner_client)
            .build()
        }
    );
    tracing::info!("Successfully built Online Store REST client.");
    ping_rondb_rest_server().await.expect("Failed to ping Online Store REST server.");
    tracing::info!("Successfully pinged Online Store REST server.");
    Ok(())
}

