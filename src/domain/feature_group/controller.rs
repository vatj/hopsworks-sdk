use color_eyre::Result;
use polars::prelude::{DataFrame, Schema};

use crate::{
    domain::{feature, job, kafka},
    kafka_producer::produce_df,
    repositories::{
        feature::payloads::NewFeaturePayload,
        feature_group::{self, entities::FeatureGroupDTO, payloads::NewFeatureGroupPayload},
        project::service::get_client_project,
    },
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
    features: Vec<NewFeaturePayload>,
    event_time: Option<&'a str>,
) -> NewFeatureGroupPayload<'a> {
    NewFeatureGroupPayload::new(name, version, description, features, event_time)
}

pub async fn insert_in_registered_feature_group(
    feature_group_name: &str,
    feature_group_version: i32,
    online_topic_name: &str,
    dataframe: &mut DataFrame,
    primary_keys: Vec<&str>,
) -> Result<()> {
    let brokers = kafka::controller::get_project_broker_endpoints(true).await?;

    let broker = brokers.first().unwrap();

    let project_name = get_client_project().await?.project_name;

    produce_df(
        dataframe,
        broker,
        online_topic_name,
        project_name.as_str(),
        primary_keys,
    )
    .await?;

    let job_name = format!(
        "{}_{}_offline_fg_backfill",
        feature_group_name, feature_group_version
    );

    let _running_job_dto = job::controller::run_job_with_name(job_name.as_str()).await?;

    Ok(())
}

pub fn build_new_feature_group_payload<'a>(
    name: &'a str,
    version: i32,
    description: Option<&'a str>,
    primary_key: Vec<&'a str>,
    event_time: Option<&'a str>,
    schema: Schema,
) -> Result<NewFeatureGroupPayload<'a>> {
    let features =
        feature::controller::build_feature_payloads_from_schema_and_feature_group_options(
            schema,
            primary_key,
        )
        .unwrap();

    Ok(NewFeatureGroupPayload::new(
        name,
        version,
        description,
        features,
        event_time,
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
