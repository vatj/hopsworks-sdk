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
use crate::get_hopsworks_client;
use crate::repositories::kafka::entities::KafkaSubjectDTO;
use crate::repositories::storage_connector::entities::FeatureStoreKafkaConnectorDTO;

async fn setup_future_producer(
    kafka_connector: FeatureStoreKafkaConnectorDTO,
) -> Result<FutureProducer> {
    let cert_dir = get_hopsworks_client()
        .await
        .get_cert_dir()
        .lock()
        .await
        .clone();
    let bootstrap_servers =
        std::env::var("HOPSWORKS_KAFKA_BROKERS").unwrap_or(kafka_connector.bootstrap_servers);
    Ok(ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "300000")
        .set("security.protocol", "SSL")
        .set("ssl.endpoint.identification.algorithm", "none")
        .set("ssl.ca.location", format!("{cert_dir}/ca_chain.pem"))
        .set(
            "ssl.certificate.location",
            format!("{cert_dir}/client_cert.pem"),
        )
        .set("ssl.key.location", format!("{cert_dir}/client_key.pem"))
        // jks truststore not supported by rdkafka, get cert key from Hopsworks client
        // .set("debug", "all")
        .create()
        .expect("Error setting up kafka producer"))
}

pub async fn produce_df(
    df: &mut polars::prelude::DataFrame,
    kafka_connector: FeatureStoreKafkaConnectorDTO,
    subject_name: &str,
    opt_version: Option<&str>,
    online_topic_name: &str,
    primary_keys: Vec<&str>,
    feature_group_id: i32,
) -> Result<()> {
    let producer: &FutureProducer = &setup_future_producer(kafka_connector).await?;

    let subject_dto: KafkaSubjectDTO = get_kafka_topic_subject(subject_name, opt_version).await?;

    let avro_schema = Schema::parse_str(subject_dto.schema.as_str()).unwrap();

    let project_id = get_hopsworks_client()
        .await
        .get_project_id()
        .lock()
        .await
        .unwrap()
        .to_string();
    let subject_id = subject_dto.id.to_string();
    let feature_group_id = feature_group_id.to_string();

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
        let topic_name = online_topic_name.to_string();
        let project_id = project_id.clone();
        let feature_group_id = feature_group_id.clone();
        let subject_id = subject_id.clone();

        handles.spawn(async move {
            let produce_future = producer.send(
                make_future_record_from_encoded(
                    &the_key,
                    &encoded_payload,
                    &topic_name,
                    &project_id,
                    &feature_group_id,
                    &subject_id,
                )
                .unwrap(),
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

    producer.flush(Duration::from_secs(1))?;

    Ok(())
}

fn make_future_record_from_encoded<'a>(
    the_key: &'a String,
    encoded_payload: &'a Vec<u8>,
    topic_name: &'a str,
    project_id: &str,
    feature_group_id: &str,
    subject_id: &str,
) -> Result<FutureRecord<'a, String, Vec<u8>>> {
    let version_str = String::from("1");
    Ok(FutureRecord::to(topic_name)
        .payload(encoded_payload)
        .key(the_key)
        .headers(
            OwnedHeaders::new()
                .insert(Header {
                    key: "version",
                    value: Some(&version_str),
                })
                .insert(Header {
                    key: "projectId",
                    value: Some(project_id),
                })
                .insert(Header {
                    key: "featureGroupId",
                    value: Some(feature_group_id),
                })
                .insert(Header {
                    key: "subjectId",
                    value: Some(subject_id),
                }),
        ))
}
