use apache_avro::types::Record;
use apache_avro::Schema;
use color_eyre::Result;
use indicatif::ProgressBar;
use polars::prelude::*;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::ClientConfig;
use std::time::Duration;
use tokio::task::JoinSet;

use crate::domain::kafka::controller::get_kafka_topic_subject;
use crate::repositories::kafka::entities::KafkaSubjectDTO;

async fn setup_future_producer(broker: &str, project_name: &str) -> Result<FutureProducer> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "300000")
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
    df: &mut polars::prelude::DataFrame,
    broker: &str,
    topic_name: &str,
    project_name: &str,
    primary_keys: Vec<&str>,
) -> Result<()> {
    let producer: &FutureProducer = &setup_future_producer(broker, project_name).await?;

    let subject_dto: KafkaSubjectDTO = get_kafka_topic_subject(topic_name).await?;

    let avro_schema = Schema::parse_str(subject_dto.schema.as_str()).unwrap();

    df.as_single_chunk_par();

    let column_names = df.get_column_names();

    let mut composite_key: Vec<String> = vec![];
    let mut row = df.get_row(0)?;
    let mut handles = JoinSet::new();

    for idx in 0..df.height() {
        let mut record = Record::new(&avro_schema).unwrap();
        df.get_row_amortized(idx, &mut row)?;

        for (jdx, value) in row.0.iter().enumerate() {
            if value.dtype() == DataType::Int64
                || value.dtype() == DataType::Datetime(TimeUnit::Nanoseconds, None)
            {
                record.put(column_names[jdx], Some(value.try_extract::<i64>()?));
                if primary_keys.contains(&column_names[jdx]) {
                    composite_key.push(value.to_string())
                }
            }

            if value.dtype() == DataType::Utf8 {
                record.put(column_names[jdx], Some(value.to_string()));
                if primary_keys.contains(&column_names[jdx]) {
                    composite_key.push(value.to_string())
                }
            }

            if value.dtype() == DataType::Float64 {
                record.put(column_names[jdx], Some(value.try_extract::<f64>()?));
                if primary_keys.contains(&column_names[jdx]) {
                    composite_key.push(value.to_string())
                }
            }
        }

        let encoded_payload = apache_avro::to_avro_datum(&avro_schema, record.clone())?;
        let the_key = composite_key.join("_");

        composite_key.clear();

        let producer = producer.clone();
        let topic_name = topic_name.to_string();

        handles.spawn(async move {
            let produce_future = producer.send(
                make_future_record_from_encoded(&the_key, &encoded_payload, &topic_name).unwrap(),
                Duration::from_secs(5),
            );

            match produce_future.await {
                Ok(_delivery) => Ok(()),
                Err((e, _)) => Err(e),
            }
        });
    }

    let progress_bar = ProgressBar::new(df.height() as u64);
    while let Some(res) = handles.join_next().await {
        progress_bar.inc(1);
        res??;
    }

    producer.flush(Duration::from_secs(0))?;

    Ok(())
}

fn make_future_record_from_encoded<'a>(
    the_key: &'a String,
    encoded_payload: &'a Vec<u8>,
    topic_name: &'a str,
) -> Result<FutureRecord<'a, String, Vec<u8>>> {
    let version_str = String::from("1");
    Ok(FutureRecord::to(topic_name)
        .payload(encoded_payload)
        .key(the_key)
        .headers(OwnedHeaders::new().insert(Header {
            key: "version",
            value: Some(&version_str),
        })))
}
