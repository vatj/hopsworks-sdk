use color_eyre::Result;
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
        .get_feature_group_by_name_and_version("rusty", 1)
        .await?
    {
        info!("{}", serde_json::to_string_pretty(&feature_group).unwrap());

        // kafka
        let topic = feature_group
            .online_topic_name
            .unwrap_or_else(|| String::from(""))
            .clone();

        let broker = "localhost:9192";

        let mini_df = get_mini_df().await?;

        info!("producing to topic '{topic}' on broker '{broker}'");

        kafka_producer::produce(broker, topic.as_ref(), &project.project_name).await?;

        kafka_producer::produce_df(
            mini_df,
            broker,
            topic.as_ref(),
            project.id,
            &project.project_name,
        )
        .await?;
    }

    Ok(())
}
