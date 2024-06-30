use color_eyre::Result;
use polars::prelude::Schema;

use hopsworks_core::{
    controller::feature_store::feature_group::{
        build_new_feature_group_payload, 
        save_feature_group_metadata
    }, 
    feature_store::FeatureGroup
};

pub async fn register_feature_group_if_needed(fg: &FeatureGroup, schema: Schema) -> Result<Option<FeatureGroup>> {
    if fg.id().is_none() {
        let payload = build_new_feature_group_payload(
            fg.name(), 
            fg.version(), 
            fg.description(), 
            fg.primary_keys()?, 
            fg.event_time(), 
            schema, 
            fg.is_online_enabled()
        )?;
        let fg_dto = save_feature_group_metadata(fg.feature_store_id(), payload).await?;
        return Ok(Some(FeatureGroup::from(fg_dto)));
    }
    Ok(None)
}
