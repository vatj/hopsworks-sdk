use color_eyre::Result;

use hopsworks_core::controller::feature_store::query;
use serde_json::json;

#[tracing::instrument]
pub fn build_find_neighbor_opensearch_query(
    size_neighbor_list: u32,
    col_name: &str,
    embedding: &str,
    sources: Vec<serde_json::Value>,
    filters: Vec<serde_json::Value>,
) -> Result<serde_json::Value> {
    let mut must_list = Vec::with_capacity(filters.len() + 2);
    must_list.push(json!({ "knn": {col_name: { "vector": embedding, "k": size_neighbor_list } } }));
    must_list.push(json!({ "exists": { "field": col_name } }));
    must_list.extend(filters);

    let query = json!({
        "size": size_neighbor_list,
        "query": {
            "bool": {
                "must": [
                    { "knn": {col_name: { "vector": embedding, "k": size_neighbor_list } } },
                    { "exists": { "field": col_name } }
                ]
            }
        },
        "_source": sources
    });

    Ok(query)
}

#[tracing::instrument]
pub fn build_fg_show_query(n_show: u32, primary_keys: &[String]) -> Result<serde_json::Value> {
    let query = json!({
        "size": n_show,
        "query": {
            "bool": {
                "must": [
                    { "exists": { "field": primary_keys } }
                ]
            }
        },
        "_source": primary_keys
    });

    Ok(query)
}

#[tracing::instrument]
pub fn build_get_feature_vectors_query(
    n_entries: u32,
    entries: Vec<serde_json::Value>,
) -> Result<serde_json::Value> {
    let query = json!({
        "size": n_entries,
        "query": {
            "bool": {
                "must": [
                    { "match": entries }
                ]
            }
        }
    });

    Ok(query)
}
