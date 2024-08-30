use color_eyre::Result;

use serde_json::json;

#[tracing::instrument]
pub fn build_find_neighbor_opensearch_query(
    size_neighbor_list: u32,
    col_name: &str, 
    embedding: &str, 
    sources: Vec<serde_json::Value>, 
    filters: Vec<serde_json::Value>) -> Result<serde_json::Value> {
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

