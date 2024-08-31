use color_eyre::Result;

use crate::controller::feature_store::feature;

use crate::cluster_api::feature_store::{
    feature::payloads::NewFeaturePayload,
    feature_group::{self, payloads::NewFeatureGroupPayload, FeatureGroupDTO},
};

pub async fn get_feature_group_by_name_and_version(
    feature_store_id: i32,
    name: &str,
    version: Option<i32>,
) -> Result<Option<FeatureGroupDTO>> {
    if version.is_none() {
        return feature_group::service::get_latest_feature_group_by_name(feature_store_id, name)
            .await;
    }
    feature_group::service::get_feature_group_by_name_and_version(
        feature_store_id,
        name,
        version.unwrap(),
    )
    .await
}

pub async fn create_feature_group(
    feature_store_id: i32,
    new_feature_group_payload: NewFeatureGroupPayload,
) -> Result<FeatureGroupDTO> {
    feature_group::service::create_feature_group(feature_store_id, &new_feature_group_payload).await
}

pub fn make_new_feature_group_payload(
    name: &str,
    version: i32,
    description: Option<&str>,
    features: Vec<NewFeaturePayload>,
    event_time: Option<&str>,
    online_enabled: bool,
) -> NewFeatureGroupPayload {
    NewFeatureGroupPayload::new(
        name,
        version,
        description,
        features,
        event_time,
        online_enabled,
    )
}

pub fn build_new_feature_group_payload(
    name: &str,
    version: i32,
    description: Option<&str>,
    primary_key: Vec<&str>,
    event_time: Option<&str>,
    online_enabled: bool,
    feature_names: &[String],
    feature_types: &[String],
) -> Result<NewFeatureGroupPayload> {
    let features = feature::build_feature_payloads_from_schema_and_feature_group_options(
        feature_names,
        feature_types,
        primary_key,
    )
    .unwrap();

    Ok(NewFeatureGroupPayload::new(
        name,
        version,
        description,
        features,
        event_time,
        online_enabled,
    ))
}

pub async fn save_feature_group_metadata(
    feature_store_id: i32,
    new_feature_group_payload: NewFeatureGroupPayload,
) -> Result<FeatureGroupDTO> {
    let feature_group_dto =
        feature_group::service::create_feature_group(feature_store_id, &new_feature_group_payload)
            .await?;

    Ok(feature_group_dto)
}

pub async fn delete_feature_group(feature_store_id: i32, feature_group_id: i32) -> Result<()> {
    feature_group::service::delete_feature_group(feature_store_id, feature_group_id).await
}
