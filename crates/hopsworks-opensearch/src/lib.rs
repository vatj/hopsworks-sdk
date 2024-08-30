use color_eyre::Result;
use opensearch::OpenSearch;
use opensearch::http::{transport::{SingleNodeConnectionPool, TransportBuilder}, Url};
use std::sync::OnceLock;

pub mod vector_db;

static HOPSWORKS_OPENSEARCH_CLIENT : OnceLock<OpenSearch> = OnceLock::new();

fn get_hopsworks_opensearch_client() -> Result<&'static OpenSearch> {
    match HOPSWORKS_OPENSEARCH_CLIENT.get() {
        Some(the_client) => Ok(the_client),
        None => color_eyre::eyre::bail!("Hopsworks OpenSearch Client not initialized. Call init_hopsworks_opensearch_client() first.")
    }
}

#[tracing::instrument(skip(token))]
pub fn init_hopsworks_opensearch_client(url: &str, token: &str) -> Result<()> {
    let url = Url::parse(url)?;
    let conn_pool = SingleNodeConnectionPool::new(url);
    let bearer_token = opensearch::auth::Credentials::Bearer(token.to_string());
    let transport = TransportBuilder::new(conn_pool).auth(bearer_token).disable_proxy().build()?;
    HOPSWORKS_OPENSEARCH_CLIENT.get_or_init(|| OpenSearch::new(transport));
    Ok(())
}


