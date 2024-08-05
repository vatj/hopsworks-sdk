use color_eyre::Result;
use tracing::debug;
use tokio::sync::OnceCell;

mod client;
mod rondb_feature_store_api;
mod entities;
mod payload;

use client::{OnlineStoreRestClient, OnlineStoreRestClientBuilder};

static RONDB_REST_CLIENT: OnceCell<OnlineStoreRestClient> = OnceCell::const_new();

pub async fn get_rondb_rest_client() -> &'static OnlineStoreRestClient {
    debug!("Access global RonDB REST Client");
    match RONDB_REST_CLIENT.get() {
        Some(client) => client,
        None => panic!(
            "First use hopsworks_online_store::inint_rondb_rest_client() to initialize the RonDB REST client with your credentials."
        ),
    }
}

pub async fn init_rondb_rest_client(client_builder: Option<OnlineStoreRestClientBuilder>) -> Result<()> {
    &RONDB_REST_CLIENT
        .get_or_try_init(|| async { client_builder.unwrap_or_default().build().await })
        .await?;
    Ok(())
}