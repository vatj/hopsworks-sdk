use color_eyre::Result;
use polars::prelude::DataFrame;

use hopsworks_core::{feature_store::FeatureGroup, get_hopsworks_client, platform::job_execution::JobExecution};
use hopsworks_kafka::insert::insert_in_registered_feature_group;


pub async fn insert_polars_df_into_kafka(dataframe: &mut DataFrame, fg: &FeatureGroup) -> Result<JobExecution> {
    let cert_dir_path = get_hopsworks_client().await.get_cert_dir().lock().await.clone();
    insert_in_registered_feature_group(
        dataframe,
        fg.feature_store_id(),
        fg.id().expect("Register the Feature Group first"),
        fg.name(),
        fg.version(),
        fg.online_topic_name().expect("Register the Feature Group first"),
        fg.primary_keys()?.as_slice(),
        cert_dir_path.as_str(),
    )
    .await
}