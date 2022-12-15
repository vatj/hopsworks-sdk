use color_eyre::Result;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::env;
use std::time::Duration;

pub mod client;
pub mod minidf;
pub mod models;

use log::info;

use crate::minidf::get_example_df;
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
    let feature_group_id: i32 = 13;

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

    let df = get_example_df().await?;

    info!("{:?}", df);

    // kafka

    let (version_n, version_s) = rdkafka::util::get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topic = "hello";
    let brokers = "localhost:9092";

    info!("producing to topic '{topic}' on broker '{brokers}'");

    // produce(brokers, topic).await;

    Ok(())
}

async fn produce(brokers: &str, topic_name: &str) {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    // This loop is non blocking: all messages will be sent one after the other, without waiting
    // for the results.
    let futures = (0..5)
        .map(|i| async move {
            // The send operation on the topic returns a future, which will be
            // completed once the result or failure from Kafka is received.
            let delivery_status = producer
                .send(
                    FutureRecord::to(topic_name)
                        .payload(&format!("Message {}", i))
                        .key(&format!("Key {}", i))
                        .headers(OwnedHeaders::new().insert(Header {
                            key: "header_key",
                            value: Some("header_value"),
                        })),
                    Duration::from_secs(0),
                )
                .await;

            // This will be executed when the result is received.
            info!("Delivery status for message {} received", i);
            delivery_status
        })
        .collect::<Vec<_>>();

    // This loop will wait until all delivery statuses have been received.
    for future in futures {
        info!("Future completed. Result: {:?}", future.await);
    }
}
