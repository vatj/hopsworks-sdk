use color_eyre::Result;
use polars::prelude::{DataFrame, Schema};

use crate::{
    core::{
        feature_store::{feature, storage_connector},
        platform::job_execution,
    },
    kafka_producer::produce_df,
    repositories::feature_store::{
        feature::payloads::NewFeaturePayload,
        feature_group::{self, entities::FeatureGroupDTO, payloads::NewFeatureGroupPayload},
    },
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
    new_feature_group_payload: NewFeatureGroupPayload<'_>,
) -> Result<FeatureGroupDTO> {
    feature_group::service::create_feature_group(feature_store_id, &new_feature_group_payload).await
}

pub fn make_new_feature_group_payload<'a>(
    name: &'a str,
    version: i32,
    description: Option<&'a str>,
    features: Vec<NewFeaturePayload>,
    event_time: Option<&'a str>,
    online_enabled: bool,
) -> NewFeatureGroupPayload<'a> {
    NewFeatureGroupPayload::new(
        name,
        version,
        description,
        features,
        event_time,
        online_enabled,
    )
}

pub async fn insert_in_registered_feature_group(
    feature_store_id: i32,
    feature_group_id: i32,
    feature_group_name: &str,
    feature_group_version: i32,
    online_topic_name: &str,
    dataframe: &mut DataFrame,
    primary_keys: Vec<String>,
) -> Result<()> {
    let kafka_connector =
        storage_connector::get_feature_store_kafka_connector(feature_store_id, true).await?;
    let ref_primary_keys = primary_keys.iter().map(|key| key.as_str()).collect();
    let subject_name = format!("{}_{}", feature_group_name, feature_group_version);

    produce_df(
        dataframe,
        kafka_connector,
        subject_name.as_str(),
        None,
        online_topic_name,
        ref_primary_keys,
        feature_group_id,
    )
    .await?;

    let job_name = format!(
        "{}_{}_offline_fg_materialization",
        feature_group_name, feature_group_version
    );

    let _running_job_dto =
        job_execution::start_new_execution_for_named_job(job_name.as_str()).await?;

    Ok(())
}

pub fn build_new_feature_group_payload<'a>(
    name: &'a str,
    version: i32,
    description: Option<&'a str>,
    primary_key: Vec<&'a str>,
    event_time: Option<&'a str>,
    schema: Schema,
    online_enabled: bool,
) -> Result<NewFeatureGroupPayload<'a>> {
    let features =
        feature::build_feature_payloads_from_schema_and_feature_group_options(schema, primary_key)
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
    new_feature_group_payload: NewFeatureGroupPayload<'_>,
) -> Result<FeatureGroupDTO> {
    let feature_group_dto =
        feature_group::service::create_feature_group(feature_store_id, &new_feature_group_payload)
            .await?;

    Ok(feature_group_dto)
}
