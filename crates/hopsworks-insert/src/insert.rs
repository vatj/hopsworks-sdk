use color_eyre::Result;
use polars::prelude::DataFrame;

use hopsworks_core::{controller::feature_store::feature_group, feature_store::feature_group::{FeatureGroup, feature::Feature}, platform::job_execution::JobExecution};


/// Inserts or upserts data into the Feature Group table.
///
/// Dataframe is written row by row to the project Kafka topic.
/// A Hudi job is then triggered to materialize the data into the offline Feature Group table.
///
/// If the Feature Group is online enabled, Hopsworks onlineFS service
/// writes rows by primary key to RonDB. Only the most recent value for a primary key
/// is stored.
///
/// # Arguments
/// * `dataframe` - A mutable reference to a Polars DataFrame containing the data to insert.
///
/// # Returs
/// A JobExecution object containing information about status of the insertion job.
///
/// # Example
/// ```no_run
/// use color_eyre::Result;
///
/// use polars::prelude::*;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///   let project = hopsworks::login(None).await?;
///   let feature_store = project.get_feature_store().await?;
///
///   let mut feature_group = feature_store
///     .get_feature_group("my_feature_group", Some(1))
///     .await?
///     .expect("Feature Group not found");
///
///   let mut mini_df = df! [
///     "number" => [2i64, 3i64],
///     "word" => ["charlie", "dylan"]
///   ]?;
///
///  feature_group.insert(&mut mini_df).await?;
///
///  Ok(())
/// }
/// ```
pub async fn insert(fgroup : &FeatureGroup, dataframe: &mut DataFrame) -> Result<JobExecution> {
    feature_group::insert_in_registered_feature_group(
        fgroup.feature_store_id(),
        fgroup.id().unwrap(),
        fgroup.name(),
        fgroup.version(),
        fgroup.online_topic_name().unwrap_or_default(),
        dataframe,
        &fgroup.get_primary_keys()?,
    )
    .await
}