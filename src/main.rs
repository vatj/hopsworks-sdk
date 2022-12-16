use color_eyre::Result;
use models::kafka_topics::KafkaBrokersDTO;
use std::env;
pub mod client;
pub mod kafka_producer;
pub mod minidf;
pub mod models;

use log::info;

use crate::minidf::get_mini_df;
use crate::models::feature_group::FeatureGroupDTO;
use crate::models::feature_store::FeatureStoreDTO;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let the_client: client::HopsworksClient = client::HopsworksClient::default();

    let email = env::var("HOPSWORKS_EMAIL").unwrap_or_default();
    let password = env::var("HOPSWORKS_PASSWORD").unwrap_or_default();
    let api_key = env::var("HOPSWORKS_API_KEY").unwrap_or_default();

    if email.len() > 1 && password.len() > 1 {
        the_client
            .login_with_email_and_password(&email, &password)
            .await?;
    } else if api_key.len() > 1 {
        the_client.set_api_key(api_key).await?;
    } else {
        panic!("Use a combination of email and password or an API key to authenticate.")
    }

    let project_id: i32 = 119;
    let feature_store_id: i32 = 67;
    let feature_group_id: i32 = 39;

    let feature_store: FeatureStoreDTO = the_client
        .get(format!("project/{project_id}/featurestores/{feature_store_id}").as_str())
        .await?
        .json()
        .await?;

    info!("{}", serde_json::to_string_pretty(&feature_store).unwrap());

    let feature_group : FeatureGroupDTO = the_client
        .get(format!("project/{project_id}/featurestores/{feature_store_id}/featuregroups/{feature_group_id}").as_str())
        .await?
        .json()
        .await?;

    info!("{}", serde_json::to_string_pretty(&feature_group).unwrap());

    //polars

    let df = get_mini_df().await?;

    info!("{:?}", df);

    // kafka

    let (version_n, version_s) = rdkafka::util::get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topic = feature_group
        .online_topic_name
        .unwrap_or_else(|| String::from(""))
        .clone();
    let brokers: KafkaBrokersDTO = the_client
        .get(format!("project/{project_id}/kafka/clusterinfo?external=True").as_str())
        .await?
        .json()
        .await?;

    if !brokers.brokers.is_empty() {
        let broker = brokers.brokers[0].replace("EXTERNAL://", "");

        info!("producing to topic '{topic}' on broker '{broker}'");

        kafka_producer::produce(broker.as_ref(), topic.as_ref()).await;
    }

    Ok(())
}
