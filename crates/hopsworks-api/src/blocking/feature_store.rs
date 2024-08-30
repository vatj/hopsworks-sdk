use color_eyre::Result;

use hopsworks_core::feature_store::{FeatureStore, FeatureGroup, FeatureView, query::Query, embedding::EmbeddingIndex};
use hopsworks_core::get_hopsworks_runtime;

#[cfg(feature = "blocking")]
pub fn get_feature_group_blocking(fs: &FeatureStore, name: &str, version: Option<i32>, multithreaded: bool) -> Result<Option<FeatureGroup>> {
    let rt = get_hopsworks_runtime(multithreaded).clone();
    let _guard = rt.enter();

    rt.block_on(fs.get_feature_group(name, version))
}

#[cfg(feature = "blocking")]
pub fn get_or_create_feature_group_blocking(
    fs: &FeatureStore, 
    name: &str, 
    version: Option<i32>, 
    description: Option<&str>, 
    primary_key: Vec<&str>, 
    event_time: Option<&str>, 
    online_enabled: bool, 
    embedding_index: Option<EmbeddingIndex>, 
    multithreaded: bool) -> Result<FeatureGroup> {
    let rt = get_hopsworks_runtime(multithreaded).clone();
    let _guard = rt.enter();

    if let Some(fgroup) = rt.block_on(fs.get_feature_group(name, version))? {
        Ok(fgroup)
    } else {
        Ok(fs.feature_group_builder()
            .name(String::from(name))
            .version(version.unwrap_or(1))
            .description(description.map(String::from))
            .event_time(event_time.map(String::from))
            .primary_key(primary_key.iter().map(|s| String::from(*s)).collect())
            .online_enabled(online_enabled)
            .embedding_index(embedding_index)
            .build())
    }
}

#[cfg(feature = "blocking")]
pub fn get_feature_view_blocking(fs: &FeatureStore, name: &str, version: Option<i32>, multithreaded: bool) -> Result<Option<FeatureView>> {
    let rt = get_hopsworks_runtime(multithreaded).clone();
    let _guard = rt.enter();

    rt.block_on(fs.get_feature_view(name, version))
}

#[cfg(feature = "blocking")]
pub fn create_feature_view_blocking(fs: &FeatureStore, name: &str, version: i32, query: Query, description: Option<&str>, multithreaded: bool) -> Result<FeatureView> {
    let rt = get_hopsworks_runtime(multithreaded).clone();
    let _guard = rt.enter();

    rt.block_on(fs.create_feature_view(name, version, query, None, description))
}