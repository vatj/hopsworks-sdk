use apache_avro::types::Record;
use apache_avro::{Schema, Writer};
use color_eyre::Result;
use log::{debug, info};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::time::Duration;

use crate::domain::kafka::controller::get_kafka_topic_subject;
use crate::repositories::kafka::entities::KafkaSubjectDTO;

pub async fn produce(broker: &str, topic_name: &str, project_name: &str) -> Result<()> {
    let producer: &FutureProducer = &setup_future_producer(broker, project_name).await?;

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

    Ok(())
}

async fn setup_future_producer(broker: &str, project_name: &str) -> Result<FutureProducer> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000")
        .set("security.protocol", "SSL")
        .set(
            "ssl.ca.location",
            format!("/tmp/{project_name}/ca_chain.pem"),
        )
        .set(
            "ssl.certificate.location",
            format!("/tmp/{project_name}/client_cert.pem"),
        )
        .set(
            "ssl.key.location",
            format!("/tmp/{project_name}/client_key.pem"),
        )
        // .set("debug", "broker,msg,security")
        .create()
        .expect("Error setting up kafka producer"))
}

pub async fn produce_df(
    df: polars::prelude::DataFrame,
    broker: &str,
    topic_name: &str,
    project_id: i32,
    project_name: &str,
) -> Result<()> {
    let producer: &FutureProducer = &setup_future_producer(broker, project_name).await?;

    let subject_dto: KafkaSubjectDTO = get_kafka_topic_subject(project_id, topic_name).await?;

    let the_schema = Schema::parse_str(subject_dto.schema.as_str()).unwrap();

    // This loop is non blocking: all messages will be sent one after the other, without waiting
    // for the results.

    let futures = (0..1)
        .map(|i| {
            let local_schema = the_schema.clone();

            async move {
                let the_key = String::from("2");
                let encoded_payload = make_encoded_record_from_row(local_schema).unwrap();
                // The send operation on the topic returns a future, which will be
                // completed once the result or failure from Kafka is received.
                let delivery_status = producer
                    .send(
                        make_future_record_from_encoded(&the_key, &encoded_payload, topic_name)
                            .unwrap(),
                        Duration::from_secs(1),
                    )
                    .await;

                // This will be executed when the result is received.
                info!("Delivery status for message {} received", i);
                delivery_status
            }
        })
        .collect::<Vec<_>>();

    // This loop will wait until all delivery statuses have been received.
    for future in futures {
        info!("Future completed. Result: {:?}", future.await);
    }

    Ok(())
}

fn make_encoded_record_from_row(the_schema: Schema) -> Result<Vec<u8>> {
    let mut writer = Writer::new(&the_schema, Vec::new());

    let mut record = Record::new(&the_schema).unwrap();
    record.put("number", Some(2i64));
    record.put("word", Some("carl"));

    writer.append(record).unwrap();

    Ok(writer.into_inner()?)
}

fn make_future_record_from_encoded<'a>(
    the_key: &'a String,
    encoded_payload: &'a Vec<u8>,
    topic_name: &'a str,
) -> Result<FutureRecord<'a, String, Vec<u8>>> {
    Ok(FutureRecord::to(topic_name)
        .payload(encoded_payload)
        .key(the_key)
        .headers(OwnedHeaders::new().insert(Header {
            key: "version",
            value: Some("1"),
        })))
}
