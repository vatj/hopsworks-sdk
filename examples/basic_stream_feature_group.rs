use color_eyre::Result;
use hopsworks_rs::domain::job;
use hopsworks_rs::domain::storage_connector::controller::get_feature_store_kafka_connector;
use hopsworks_rs::minidf::get_mini_df;
use log::info;

use hopsworks_rs::hopsworks_login;
use hopsworks_rs::kafka_producer;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let project = hopsworks_login()
        .await
        .expect("Error connecting to Hopsworks:\n");

    let feature_store = project
        .get_feature_store()
        .await
        .expect("All projects should have a default feature store");

    info!("{}", serde_json::to_string_pretty(&feature_store).unwrap());

    if let Some(feature_group) = feature_store
        .get_feature_group_by_name_and_version("bob", 1)
        .await?
    {
        info!("{}", serde_json::to_string_pretty(&feature_group).unwrap());

        // kafka
        let topic = feature_group
            .get_online_topic_name()
            .unwrap_or_else(|| String::from(""));
        let kafka_connector = get_feature_store_kafka_connector(feature_store.featurestore_id, true).await?;

        let mut mini_df = get_mini_df().await?;

        info!("producing to topic '{topic}' on broker '{}'", kafka_connector.bootstrap_servers);

        let primary_keys = feature_group.get_primary_keys()?;

        kafka_producer::produce_df(
            &mut mini_df,
            kafka_connector,
            topic.as_ref(),
            None,
            &project.project_name,
            &project.project_name,
            primary_keys.iter().map(|key| key.as_str()).collect(),
        )
        .await?;

        let job_name = format!(
            "{}_{}_offline_fg_materialization",
            feature_group.name, feature_group.version
        );

        let _running_job_dto = job::controller::run_job_with_name(job_name.as_str()).await?;
    }

    Ok(())
}
