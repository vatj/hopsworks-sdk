use color_eyre::Result;
use polars::prelude::*;
use apache_avro::Schema;
use std::sync::Arc;

use hopsworks_core::controller::feature_store::storage_connector;
use hopsworks_core::platform::job_execution::JobExecution;
use hopsworks_core::controller::platform::job_execution;
use hopsworks_core::controller::platform::kafka::get_kafka_topic_subject;
use hopsworks_core::get_hopsworks_client;

use crate::kafka_producer::setup_future_producer;
use crate::helper::make_custom_headers;
use crate::produce_polars_df::produce_df;

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
    let future_producer = setup_future_producer(kafka_connector, cert_dir).await?;

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
        primary_keys,
        Schema::parse_str(subject.schema())?,
        &future_producer,
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

// use color_eyre::Result;
// use polars::prelude::DataFrame;

// use hopsworks_core::{feature_store::feature_group::FeatureGroup, platform::job_execution::JobExecution};

// /// Inserts or upserts data into the Feature Group table.
// ///
// /// Dataframe is written row by row to the project Kafka topic.
// /// A Hudi job is then triggered to materialize the data into the offline Feature Group table.
// ///
// /// If the Feature Group is online enabled, Hopsworks onlineFS service
// /// writes rows by primary key to RonDB. Only the most recent value for a primary key
// /// is stored.
// ///
// /// # Arguments
// /// * `dataframe` - A mutable reference to a Polars DataFrame containing the data to insert.
// ///
// /// # Returs
// /// A JobExecution object containing information about status of the insertion job.
// ///
// /// # Example
// /// ```no_run
// /// use color_eyre::Result;
// ///
// /// use polars::prelude::*;
// ///
// /// #[tokio::main]
// /// async fn main() -> Result<()> {
// ///   let project = hopsworks::login(None).await?;
// ///   let feature_store = project.get_feature_store().await?;
// ///
// ///   let mut feature_group = feature_store
// ///     .get_feature_group("my_feature_group", Some(1))
// ///     .await?
// ///     .expect("Feature Group not found");
// ///
// ///   let mut mini_df = df! [
// ///     "number" => [2i64, 3i64],
// ///     "word" => ["charlie", "dylan"]
// ///   ]?;
// ///
// ///  feature_group.insert(&mut mini_df).await?;
// ///
// ///  Ok(())
// /// }
// /// ```
// pub async fn insert_in_registered_feature_group(fgroup : &FeatureGroup, cert_dir: &str, dataframe: &mut DataFrame) -> Result<JobExecution> {

//     crate::polars_insert::insert_in_registered_feature_group(
//         fgroup.feature_store_id(),
//         fgroup.id().unwrap(),
//         fgroup.name(),
//         fgroup.version(),
//         fgroup.online_topic_name().unwrap_or_default(),
//         &fgroup.get_primary_keys()?,
//         dataframe,
//         cert_dir,
//     )
//     .await
// }
