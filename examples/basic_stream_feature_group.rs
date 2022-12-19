use color_eyre::Result;
use log::info;

use hopsworks_rs::hopsworks_login;
use hopsworks_rs::kafka_producer;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let project = hopsworks_login().await.expect("Create a project first.");

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

        let (version_n, version_s) = rdkafka::util::get_rdkafka_version();
        info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

        let topic = feature_group
            .online_topic_name
            .unwrap_or_else(|| String::from(""))
            .clone();

        // let brokers: KafkaBrokersDTO = the_client
        //     .get(format!("project/{project_id}/kafka/clusterinfo?external=True").as_str())
        //     .await?
        //     .json()
        //     .await?;

        // if !brokers.brokers.is_empty() {
        // let broker = brokers.brokers[0].replace("EXTERNAL://", "");
        let broker = "localhost:9192";

        info!("producing to topic '{topic}' on broker '{broker}'");

        kafka_producer::produce(broker, topic.as_ref()).await?;
        // }
    }

    Ok(())
}
