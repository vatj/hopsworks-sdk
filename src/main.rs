use apache_avro::types::Record;
use apache_avro::{Schema, Writer};
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

use crate::minidf::get_mini_df;
use crate::models::feature_group::FeatureGroupDTO;
use crate::models::feature_store::FeatureStoreDTO;
use crate::models::kafka_topics::KafkaTopicListDTO;

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

    //polars

    let df = get_mini_df().await?;

    info!("{:?}", df);

    // kafka

    let kafka_topics: KafkaTopicListDTO = the_client
        .get(format!("project/{project_id}/kafka/topics/").as_str())
        .await?
        .json()
        .await?;

    let (version_n, version_s) = rdkafka::util::get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    if kafka_topics.count >= 1 {
        let topic = "119_39_rusty_1";
        let brokers = "localhost:9192";
        // let topic = "155_8559_rusty_1_onlinefs";
        // let brokers = "172.16.4.235:9091";

        info!("producing to topic '{topic}' on broker '{brokers}'");

        produce(brokers, topic).await;
    }

    Ok(())
}

async fn produce(brokers: &str, topic_name: &str) {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .set("security.protocol", "SSL")
        .set("ssl.ca.location", "/tmp/ca_chain.pem")
        .set("ssl.certificate.location", "/tmp/client_cert.pem")
        .set("ssl.key.location", "/tmp/client_key.pem")
        // .set("debug", "broker,msg,security")
        .create()
        .expect("Producer creation error");

    // This loop is non blocking: all messages will be sent one after the other, without waiting
    // for the results.
    let futures = (0..1)
        .map(|i| async move {
            let raw_schema = r#"
            {
                "type":"record",
                "name":"rusty_1",
                "namespace":"dataval_featurestore.db",
                "fields":[
                    {"name":"number","type":["null","long"]},
                    {"name":"words","type":["null","string"]}
                ]
            }"#;

            let the_schema = Schema::parse_str(raw_schema).unwrap();

            let mut writer = Writer::new(&the_schema, Vec::new());
            let mut record = Record::new(writer.schema()).unwrap();
            record.put("number", Some(2i64));
            record.put("words", Some("carl"));

            writer.append(record).unwrap();

            let encoded = writer.into_inner().unwrap();
            // The send operation on the topic returns a future, which will be
            // completed once the result or failure from Kafka is received.
            let delivery_status = producer
                .send(
                    FutureRecord::to(topic_name)
                        // .payload(&format!("Message {}", i))
                        .payload(&encoded)
                        .key(&String::from("2"))
                        .headers(OwnedHeaders::new().insert(Header {
                            key: "version",
                            value: Some("1"),
                        })),
                    Duration::from_secs(1),
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
