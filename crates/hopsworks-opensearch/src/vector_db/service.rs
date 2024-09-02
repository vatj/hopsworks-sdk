use color_eyre::Result;
use opensearch::SearchParts;

use crate::get_hopsworks_opensearch_client;

async fn find_neighbors(
    embedding_index: &str,
    knn_query: serde_json::Value,
) -> Result<Vec<serde_json::Value>> {
    let client = get_hopsworks_opensearch_client()?;
    let response = client
        .search(SearchParts::Index(&[embedding_index]))
        .body(knn_query)
        .send()
        .await?;

    match response.status_code().is_success() {
        true => {
            let body = response.json::<serde_json::Value>().await?;
            let hits = body["hits"]["hits"].as_array().unwrap();
            Ok(hits.to_owned())
        }
        false => todo!(),
    }
}

async fn get_feature_vectors(
    embedding_index: &str,
    get_feature_vectors_query: serde_json::Value,
) -> Result<serde_json::Value> {
    let client = get_hopsworks_opensearch_client()?;
    let response = client
        .search(SearchParts::Index(&[embedding_index]))
        .body(get_feature_vectors_query)
        .send()
        .await?;

    match response.status_code().is_success() {
        true => {
            let body = response.json::<serde_json::Value>().await?;
            let hits = body["hits"]["hits"].as_array().unwrap();
            Ok(hits[0].to_owned())
        }
        false => todo!(),
    }
}
