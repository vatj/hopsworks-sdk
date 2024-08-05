use color_eyre::Result;
use polars::prelude::*;
use std::sync::Arc;

use hopsworks_core::controller::feature_store::storage_connector;
use hopsworks_core::platform::job_execution::JobExecution;
use hopsworks_core::controller::platform::job_execution;
use hopsworks_core::controller::platform::kafka::get_kafka_topic_subject;
use hopsworks_core::get_hopsworks_client;

use crate::kafka_producer::setup_kafka_configuration;
use crate::helper::make_custom_headers;
use crate::produce_polars_df::produce_df;

#[tracing::instrument(
    skip(dataframe),
    fields(df_rows = dataframe.height(), df_columns = dataframe.width()))]
pub async fn insert_in_registered_feature_group(
    dataframe: &mut DataFrame,
    feature_store_id: i32,
    feature_group_id: i32,
    feature_group_name: &str,
    feature_group_version: i32,
    online_topic_name: &str,
    primary_keys: &[&str],
    cert_dir: &str,
) -> Result<JobExecution> {
    let kafka_connector =
        storage_connector::get_feature_store_kafka_connector(feature_store_id, true).await?;
    let future_producer = setup_kafka_configuration(kafka_connector, cert_dir).await?;

    let subject = get_kafka_topic_subject(format!("{}_{}", feature_group_name, feature_group_version).as_str(), None).await?;
    let project_id = get_hopsworks_client()
            .await
            .get_project_id()
            .lock()
            .await
            .expect("Project ID not set, login to Hopsworks to set it");

    // These value are wrapped into an Arc to allow read-only access across threads
    // meaning clone only increases the ref count, no extra-memory is allocated
    let topic_name = Arc::new(online_topic_name.to_string());
    let headers = make_custom_headers(
        project_id,
        feature_group_id,
        subject.id(),
        feature_group_version,
    );

    produce_df(
        headers,
        topic_name,
        primary_keys.to_vec(),
        future_producer,
        dataframe,
    )
    .await?;

    let job_name = format!(
        "{}_{}_offline_fg_materialization",
        feature_group_name, feature_group_version
    );

    Ok(JobExecution::from(
        job_execution::start_new_execution_for_named_job(job_name.as_str(), None).await?,
    ))
}
