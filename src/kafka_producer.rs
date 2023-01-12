use apache_avro::types::Record;
use apache_avro::Schema;
use color_eyre::Result;
use indicatif::{ProgressBar, ProgressIterator};
use log::info;
use polars::export::rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use polars::prelude::*;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::ClientConfig;
use std::time::{Duration};

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

    // let mut futures = vec![];

    df.as_single_chunk_par();

    // let polars_schema = df.schema();

    // let selected_utf8_columns: Vec<&str> = polars_schema
    //     .iter()
    //     // Filter the columns based on their data type
    //     .filter(|col| col.1 == &DataType::Utf8)
    //     // Extract the column names
    //     .map(|col| col.0.as_str())
    //     .collect();

    // let selected_int64_columns: Vec<&str> = polars_schema
    //     .iter()
    //     // Filter the columns based on their data type
    //     .filter(|col| col.1 == &DataType::Int64)
    //     // Extract the column names
    //     .map(|col| col.0.as_str())
    //     .collect();

    // let selected_float64_columns: Vec<&str> = polars_schema
    //     .iter()
    //     // Filter the columns based on their data type
    //     .filter(|col| col.1 == &DataType::Float64)
    //     // Extract the column names
    //     .map(|col| col.0.as_str())
    //     .collect();

    // let selected_datetime_nanoseconds_columns: Vec<&str> = polars_schema
    //     .iter()
    //     // Filter the columns based on their data type
    //     .filter(|col| col.1 == &DataType::Datetime(TimeUnit::Nanoseconds, None))
    //     // Extract the column names
    //     .map(|col| col.0.as_str())
    //     .collect();

    // let mut iters_utf8 = df
    //     .columns(selected_utf8_columns.clone())?
    //     .iter()
    //     .map(|s| Ok(s.utf8()?.into_iter()))
    //     .collect::<Result<Vec<_>>>()?;

    // let mut iters_int64 = df
    //     .columns(selected_int64_columns.clone())?
    //     .iter()
    //     .map(|s| Ok(s.i64()?.into_iter()))
    //     .collect::<Result<Vec<_>>>()?;

    // let mut iters_float64 = df
    //     .columns(selected_float64_columns.clone())?
    //     .iter()
    //     .map(|s| Ok(s.f64()?.into_iter()))
    //     .collect::<Result<Vec<_>>>()?;

    // let mut iters_datetime_nanoseconds = df
    //     .columns(selected_datetime_nanoseconds_columns.clone())?
    //     .iter()
    //     .map(|s| Ok(s.datetime()?.into_iter()))
    //     .collect::<Result<Vec<_>>>()?;

    // // This loop is blocking
    // let start_time = Instant::now();
    // let progress_bar = ProgressBar::new(df.height() as u64);
    // for _row in 0..df.height() {
    //     progress_bar.inc(1);
    //     let mut record = Record::new(&avro_schema).unwrap();
    //     let mut composite_key = vec![];

    //     for (idx, iter) in &mut iters_int64.iter_mut().enumerate() {
    //         if let Some(value) = iter.next().expect("should have as many iterations as rows") {
    //             record.put(selected_int64_columns[idx], Some(value));
    //             if primary_keys.contains(&selected_int64_columns[idx]) {
    //                 composite_key.push(value.to_string())
    //             }
    //         }
    //     }

    //     for (idx, iter) in &mut iters_utf8.iter_mut().enumerate() {
    //         if let Some(value) = iter.next().expect("should have as many iterations as rows") {
    //             record.put(selected_utf8_columns[idx], Some(String::from(value)));
    //             if primary_keys.contains(&selected_utf8_columns[idx]) {
    //                 composite_key.push(String::from(value))
    //             }
    //         }
    //     }

    //     for (idx, iter) in &mut iters_float64.iter_mut().enumerate() {
    //         if let Some(value) = iter.next().expect("should have as many iterations as rows") {
    //             record.put(selected_float64_columns[idx], Some(value));
    //             if primary_keys.contains(&selected_float64_columns[idx]) {
    //                 composite_key.push(value.to_string())
    //             }
    //         }
    //     }

    //     for (idx, iter) in &mut iters_datetime_nanoseconds.iter_mut().enumerate() {
    //         if let Some(value) = iter.next().expect("should have as many iterations as rows") {
    //             record.put(selected_datetime_nanoseconds_columns[idx], Some(value));
    //             if primary_keys.contains(&selected_datetime_nanoseconds_columns[idx]) {
    //                 composite_key.push(value.to_string())
    //             }
    //         }
    //     }

    //     let encoded_payload = apache_avro::to_avro_datum(&avro_schema, record)?;

    //     let the_key = composite_key.join("_");

    //     let delivery_status = producer
    //         .send(
    //             make_future_record_from_encoded(&the_key, &encoded_payload, topic_name)?,
    //             Duration::from_secs(0),
    //         )
    //         .await;

    //     futures.push(delivery_status);
    // }
    // let duration_casted_chunks = start_time.elapsed();

    // println!("Cast chunk took {:?}", duration_casted_chunks);

    let progress_bar = ProgressBar::new(df.height() as u64);
    let column_names = df.get_column_names();

    let mut composite_key: Vec<String> = vec![];
    let mut row = df.get_row(0)?;
    // let mut future_records = vec![];
    let mut key_and_payloads = vec![];

    for idx in 0..df.height() {
        progress_bar.inc(1);
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

        key_and_payloads.push((the_key, encoded_payload));

        // future_records.push(make_future_record_from_encoded_2(
        //     the_key,
        //     encoded_payload,
        //     topic_name,
        // )?);

        // let delivery_status = producer
        //     .send(
        //         make_future_record_from_encoded(&the_key, &encoded_payload, topic_name)?,
        //         Duration::from_secs(0),
        //     )
        //     .await;
        // futures.push(delivery_status);

        composite_key.clear();
    }

    let futures =
        // future_records
        key_and_payloads
            .iter()
            .progress()
            // .map(|future_record| async move {
            //     producer.send(future_record, Duration::from_secs(0)).await
            // })
            .map(|(key, payload)| async move {
                producer.send(
                    make_future_record_from_encoded_2(key, payload, topic_name), 
                    Duration::from_secs(0)
                ).await
            })
            .collect::<Vec<_>>();

    // This loop will wait until all delivery statuses have been received.
    let progress_bar = ProgressBar::new(futures.len() as u64);
    for future in futures {
        progress_bar.inc(1);
        // info!("Future completed. Result: {:?}", future.await);
        future.await.unwrap();

    }

    

    producer.flush(Duration::from_secs(0))?;

    Ok(())
}

// fn make_future_record_from_encoded<'a>(
//     the_key: &'a String,
//     encoded_payload: &'a Vec<u8>,
//     topic_name: &'a str,
// ) -> Result<FutureRecord<'a, String, Vec<u8>>> {
//     let version_str = String::from("1");
//     Ok(FutureRecord::to(topic_name)
//         .payload(encoded_payload)
//         .key(the_key)
//         .headers(OwnedHeaders::new().insert(Header {
//             key: "version",
//             value: Some(&version_str),
//         })))
// }

fn make_future_record_from_encoded_2<'a>(
    the_key: &'a String,
    encoded_payload: &'a Vec<u8>,
    topic_name: &'a str,
) -> FutureRecord<'a, String, Vec<u8>> {
    let version_str = String::from("1");
    FutureRecord::to(topic_name)
        .payload(encoded_payload)
        .key(the_key)
        .headers(OwnedHeaders::new().insert(Header {
            key: "version",
            value: Some(&version_str),
        }))
}
