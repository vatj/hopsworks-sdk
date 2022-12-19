use apache_avro::types::Record;
use apache_avro::{Schema, Writer};
use color_eyre::Result;
use log::info;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::time::Duration;

pub async fn produce(brokers: &str, topic_name: &str) -> Result<()> {
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

    Ok(())
}
