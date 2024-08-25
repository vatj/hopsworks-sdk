use color_eyre::Result;
use tracing;

use super::{entities::{BatchFeatureVectors, SingleFeatureVector}, payload::{BatchEntriesPayload, SingleEntryPayload}, rest_read_options::FeatureVectorRestReadOptions, rondb_feature_store_api};

#[tracing::instrument]
pub async fn get_feature_vector(feature_store_id: i32, feature_view_name: &str, feature_view_version: i32, entries: serde_json::Value, passed_values: Option<serde_json::Value>, rest_read_options: FeatureVectorRestReadOptions) -> Result<SingleFeatureVector> {
    let payload = SingleEntryPayload {
        feature_store_id,
        feature_view_name: feature_view_name.to_string(),
        feature_view_version,
        entries,
        passed_values,
        metadata_options: rest_read_options.to_metadata_options_payload(),
        options: rest_read_options.to_options_payload(),
    };

    let feature_vector_resp = rondb_feature_store_api::get_single_feature_vector(payload).await?;

    Ok(feature_vector_resp)
}

#[tracing::instrument]
pub async fn get_feature_vectors(feature_store_id: i32, feature_view_name: &str, feature_view_version: i32, entries: Vec<serde_json::Value>, passed_values: Option<Vec<serde_json::Value>>, rest_read_options: FeatureVectorRestReadOptions) -> Result<BatchFeatureVectors> {
    let payload = BatchEntriesPayload {
        feature_store_id,
        feature_view_name: feature_view_name.to_string(),
        feature_view_version,
        entries,
        passed_values: passed_values.unwrap_or_default(),
        metadata_options: rest_read_options.to_metadata_options_payload(),
        options: rest_read_options.to_options_payload(),
    };

    let feature_vector_resp = rondb_feature_store_api::get_batch_feature_vectors(payload).await?;

    Ok(feature_vector_resp)
}