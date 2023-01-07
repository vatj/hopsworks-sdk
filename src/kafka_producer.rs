use apache_avro::types::{Record, Value};
use apache_avro::Schema;
use color_eyre::Result;
use log::info;
use polars::prelude::*;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::ClientConfig;
use std::time::Duration;

use crate::domain::kafka::controller::get_kafka_topic_subject;
use crate::repositories::kafka::entities::KafkaSubjectDTO;

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
    df: &mut polars::prelude::DataFrame,
    broker: &str,
    topic_name: &str,
    project_name: &str,
    primary_keys: Vec<&str>,
) -> Result<()> {
    let producer: &FutureProducer = &setup_future_producer(broker, project_name).await?;

    let subject_dto: KafkaSubjectDTO = get_kafka_topic_subject(topic_name).await?;

    let avro_schema = Schema::parse_str(subject_dto.schema.as_str()).unwrap();

    // let primary_keys = vec!["number"];

    let mut futures = vec![];

    df.as_single_chunk_par();

    let polars_schema = df.schema();

    let selected_utf8_columns: Vec<&str> = polars_schema
        .iter()
        // Filter the columns based on their data type
        .filter(|col| col.1 == &DataType::Utf8)
        // Extract the column names
        .map(|col| col.0.as_str())
        .collect();

    let selected_int64_columns: Vec<&str> = polars_schema
        .iter()
        // Filter the columns based on their data type
        .filter(|col| col.1 == &DataType::Int64)
        // Extract the column names
        .map(|col| col.0.as_str())
        .collect();

    let selected_float64_columns: Vec<&str> = polars_schema
        .iter()
        // Filter the columns based on their data type
        .filter(|col| col.1 == &DataType::Float64)
        // Extract the column names
        .map(|col| col.0.as_str())
        .collect();

    let selected_datetime_nanoseconds_columns: Vec<&str> = polars_schema
        .iter()
        // Filter the columns based on their data type
        .filter(|col| col.1 == &DataType::Datetime(TimeUnit::Nanoseconds, None))
        // Extract the column names
        .map(|col| col.0.as_str())
        .collect();

    let mut iters_utf8 = df
        .columns(selected_utf8_columns.clone())?
        .iter()
        .map(|s| Ok(s.utf8()?.into_iter()))
        .collect::<Result<Vec<_>>>()?;

    let mut iters_int64 = df
        .columns(selected_int64_columns.clone())?
        .iter()
        .map(|s| Ok(s.i64()?.into_iter()))
        .collect::<Result<Vec<_>>>()?;

    let mut iters_float64 = df
        .columns(selected_float64_columns.clone())?
        .iter()
        .map(|s| Ok(s.f64()?.into_iter()))
        .collect::<Result<Vec<_>>>()?;

    let mut iters_datetime_nanoseconds = df
        .columns(selected_datetime_nanoseconds_columns.clone())?
        .iter()
        .map(|s| Ok(s.datetime()?.into_iter()))
        .collect::<Result<Vec<_>>>()?;

    // This loop is blocking
    for row in 0..df.height() {
        let mut record = Record::new(&avro_schema).unwrap();
        let mut composite_key = vec![];

        for (idx, iter) in &mut iters_int64.iter_mut().enumerate() {
            if let Some(value) = iter.next().expect("should have as many iterations as rows") {
                // process value
                info!(
                    "the row: {row}, the column name: {:?}, the value : {value:?}",
                    selected_int64_columns[idx]
                );
                record.put(selected_int64_columns[idx], Some(Value::Long(value)));
                if primary_keys.contains(&selected_utf8_columns[idx]) {
                    composite_key.push(value.to_string())
                }
            }
        }

        for (idx, iter) in &mut iters_utf8.iter_mut().enumerate() {
            if let Some(value) = iter.next().expect("should have as many iterations as rows") {
                // process value
                info!(
                    "the row: {row}, the column name: {:?}, the utf8 value : {value:?}",
                    selected_utf8_columns[idx]
                );
                record.put(
                    selected_utf8_columns[idx],
                    Some(Value::String(String::from(value))),
                );
                if primary_keys.contains(&selected_utf8_columns[idx]) {
                    composite_key.push(String::from(value))
                }
            }
        }

        for (idx, iter) in &mut iters_float64.iter_mut().enumerate() {
            if let Some(value) = iter.next().expect("should have as many iterations as rows") {
                // process value
                info!(
                    "the row: {row}, the column name: {:?}, the f64 value : {value:?}",
                    selected_float64_columns[idx]
                );
                record.put(selected_float64_columns[idx], Some(Value::Double(value)));
                if primary_keys.contains(&selected_float64_columns[idx]) {
                    composite_key.push(value.to_string())
                }
            }
        }

        for (idx, iter) in &mut iters_datetime_nanoseconds.iter_mut().enumerate() {
            if let Some(value) = iter.next().expect("should have as many iterations as rows") {
                // process value
                info!(
                    "the row: {row}, the column name: {:?}, the datetime value : {value:?}",
                    selected_datetime_nanoseconds_columns[idx]
                );
                record.put(
                    selected_datetime_nanoseconds_columns[idx],
                    Some(Value::Long(value)),
                );
                if primary_keys.contains(&selected_datetime_nanoseconds_columns[idx]) {
                    composite_key.push(value.to_string())
                }
            }
        }

        let encoded_payload = apache_avro::to_avro_datum(&avro_schema, record).unwrap();

        let the_key = composite_key.join("_");

        let delivery_status = producer
            .send(
                make_future_record_from_encoded(&the_key, &encoded_payload, topic_name).unwrap(),
                Duration::from_secs(0),
            )
            .await;

        futures.push(delivery_status);
    }

    // This loop will wait until all delivery statuses have been received.
    for future in futures {
        info!("Future completed. Result: {:?}", future);
    }

    producer.flush(Duration::from_secs(0)).unwrap();

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
