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
pub async fn insert(fgroup : &mut FeatureGroup, dataframe: &mut DataFrame) -> Result<JobExecution> {
    if fgroup.id().is_none() {
        let feature_group_dto = feature_group::save_feature_group_metadata(
            fgroup.featurestore_id,
            feature_group::build_new_feature_group_payload(
                &fgroup.name,
                fgroup.version,
                fgroup.description.as_deref(),
                fgroup.primary_key
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|pk| pk.as_ref())
                    .collect(),
                fgroup.event_time.as_deref(),
                dataframe.schema(),
                fgroup.online_enabled,
            )
            .unwrap(),
        )
        .await?;

        fgroup.id = Some(feature_group_dto.id);
        fgroup.online_topic_name = feature_group_dto.online_topic_name;
        fgroup.creator = Some(User::from(feature_group_dto.creator));
        fgroup.location = Some(feature_group_dto.location);
        fgroup.statistics_config = feature_group_dto
            .statistics_config
            .as_ref()
            .map(StatisticsConfig::from);
        fgroup.features_mut()
            .extend(feature_group_dto.features.into_iter().map(Feature::from));
    }

    feature_group::insert_in_registered_feature_group(
        fgroup.featurestore_id,
        fgroup.id().unwrap(),
        fgroup.name.as_str(),
        fgroup.version,
        fgroup.online_topic_name().unwrap_or_default(),
        dataframe,
        &fgroup.get_primary_keys()?,
    )
    .await
}