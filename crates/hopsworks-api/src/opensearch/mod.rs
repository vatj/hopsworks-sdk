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

#[tracing::instrument]
pub async fn get_feature_vector(
    embedding_index: &str,
    n_entries: u32,
    entries: Vec<serde_json::Value>,
) -> Result<serde_json::Value> {
    let match_query = hopsworks_opensearch::vector_db::payload::build_get_feature_vectors_query(
        n_entries, entries,
    )?;
    let feature_vector =
        hopsworks_opensearch::vector_db::service::get_feature_vectors(embedding_index, match_query)
            .await?;

    tracing::info!("Feature vector: {:?}", feature_vector);
    Ok(feature_vector)
}

#[tracing::instrument]
pub async fn find_neighbors(
    embedding_index: &str,
    size_neighbor_list: u32,
    col_name: &str,
    embedding: &str,
    sources: Vec<serde_json::Value>,
    filters: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>> {
    let knn_query = hopsworks_opensearch::vector_db::payload::build_find_neighbor_opensearch_query(
        size_neighbor_list,
        col_name,
        embedding,
        sources,
        filters,
    )?;
    let neighbors =
        hopsworks_opensearch::vector_db::service::find_neighbors(embedding_index, knn_query)
            .await?;

    tracing::info!("Neighbors: {:?}", neighbors);
    Ok(neighbors)
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

#[cfg(feature = "blocking")]
pub fn get_feature_vectors_blocking(
    embedding_index: &str,
    n_entries: u32,
    entries: Vec<serde_json::Value>,
    multithreaded: bool,
) -> Result<serde_json::Value> {
    let rt = hopsworks_core::get_hopsworks_runtime(multithreaded);
    let _guard = rt.enter();

    rt.block_on(get_feature_vector(embedding_index, n_entries, entries))
}

#[cfg(feature = "blocking")]
pub fn find_neighbors_blocking(
    embedding_index: &str,
    size_neighbor_list: u32,
    col_name: &str,
    embedding: &str,
    sources: Vec<serde_json::Value>,
    filters: Vec<serde_json::Value>,
    multithreaded: bool,
) -> Result<Vec<serde_json::Value>> {
    let rt = hopsworks_core::get_hopsworks_runtime(multithreaded);
    let _guard = rt.enter();

    rt.block_on(find_neighbors(
        embedding_index,
        size_neighbor_list,
        col_name,
        embedding,
        sources,
        filters,
    ))
}
