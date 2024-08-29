use color_eyre::Result;
use polars::prelude::Schema;

use hopsworks_core::{
    controller::feature_store::feature_group::{
        build_new_feature_group_payload, 
        save_feature_group_metadata
    }, 
    feature_store::FeatureGroup
};

#[cfg(feature="blocking")]
pub fn register_feature_group_if_needed_blocking(fg: &FeatureGroup, schema: Schema, multithreaded: bool) -> Result<Option<FeatureGroup>> {
    let rt = hopsworks_core::get_hopsworks_runtime(multithreaded).clone();
    let _guard = rt.enter();

    if fg.id().is_none() {
        let payload = build_new_feature_group_payload(
            fg.name(), 
            fg.version(), 
            fg.description(), 
            fg.primary_keys(), 
            fg.event_time(), 
            schema, 
            fg.is_online_enabled()
        )?;
        let fg_dto = rt.block_on(save_feature_group_metadata(fg.feature_store_id(), payload))?;
        return Ok(Some(FeatureGroup::from(fg_dto)));
    }
    Ok(None)
}

#[cfg(feature="blocking")]
pub fn delete_blocking(fg: &FeatureGroup, multithreaded: bool) -> Result<()> {
    let rt = hopsworks_core::get_hopsworks_runtime(multithreaded).clone();
    let _guard = rt.enter();
    
    rt.block_on(fg.delete())
}
