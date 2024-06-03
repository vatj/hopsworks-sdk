use polars::prelude::{DataFrame, Schema};
use hopsworks_insert::kafka_producer::produce_df;
use crate::platform::job_execution::JobExecution;

pub async fn insert_in_registered_feature_group(
    feature_store_id: i32,
    feature_group_id: i32,
    feature_group_name: &str,
    feature_group_version: i32,
    online_topic_name: &str,
    dataframe: &mut DataFrame,
    primary_keys: &[&str],
) -> Result<JobExecution> {
    let kafka_connector =
        storage_connector::get_feature_store_kafka_connector(feature_store_id, true).await?;
    let subject_name = format!("{}_{}", feature_group_name, feature_group_version);

    produce_df(
        dataframe,
        kafka_connector,
        subject_name.as_str(),
        None,
        online_topic_name,
        primary_keys,
        feature_group_id,
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