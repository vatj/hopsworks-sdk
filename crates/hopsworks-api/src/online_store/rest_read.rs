use color_eyre::Result;
use hopsworks_online_store::rondb_rest::entities::{BatchFeatureVectors, SingleFeatureVector};
use reqwest::header;
use hopsworks_core::{controller::platform::variables::get_loadbalancer_external_domain, feature_store::FeatureView};
pub use hopsworks_online_store::rondb_rest::rest_read_options::FeatureVectorRestReadOptions;
pub use hopsworks_online_store::rondb_rest::controller;


async fn init_online_store_rest_client(api_key: &str, api_version: &str, reqwest_client: Option<reqwest::Client>) -> Result<()> {
    let url = get_loadbalancer_external_domain("online_store_rest_server").await?;
    let header_value = header::HeaderValue::from_str(api_key)?;

    hopsworks_online_store::rondb_rest::init_online_store_rest_client(&url, header_value, api_version, reqwest_client)
}

pub async fn get_feature_vector(fview_obj: FeatureView, entry: serde_json::Value, passed_values: Option<serde_json::Value>, rest_read_options: Option<FeatureVectorRestReadOptions>) -> Result<SingleFeatureVector> {
    controller::get_feature_vector(fview_obj.feature_store_id(), fview_obj.name(), fview_obj.version(), entry, passed_values, rest_read_options.unwrap_or_default()).await
}

pub async fn get_feature_vectors(fview_obj: FeatureView, entries: Vec<serde_json::Value>, passed_values: Option<Vec<serde_json::Value>>, rest_read_options: Option<FeatureVectorRestReadOptions>) -> Result<BatchFeatureVectors> {
    controller::get_feature_vectors(fview_obj.feature_store_id(), fview_obj.name(), fview_obj.version(), entries, passed_values, rest_read_options.unwrap_or_default()).await
}