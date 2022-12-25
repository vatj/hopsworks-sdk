use color_eyre::Result;

use crate::repositories::{
    feature_group::{self, entities::FeatureGroupDTO, payloads::NewFeatureGroupPayload},
    features::payloads::NewFeaturePayload,
};

pub async fn get_feature_group_by_name_and_version(
    feature_store_id: i32,
    name: &str,
    version: i32,
) -> Result<Option<FeatureGroupDTO>> {
    feature_group::service::get_feature_group_by_name_and_version(feature_store_id, name, version)
        .await
}

pub async fn create_feature_group(
    feature_store_id: i32,
    new_feature_group_payload: NewFeatureGroupPayload<'_>,
) -> Result<FeatureGroupDTO> {
    feature_group::service::create_feature_group(feature_store_id, &new_feature_group_payload).await
}

pub fn make_new_feature_group_payload<'a>(
    name: &'a str,
    version: i32,
    description: Option<&'a str>,
    features: Vec<NewFeaturePayload<'a>>,
    primary_key: Vec<&'a str>,
    event_time: Option<&'a str>,
) -> NewFeatureGroupPayload<'a> {
    NewFeatureGroupPayload::new(
        name,
        version,
        description,
        features,
        primary_key,
        event_time,
    )
}
