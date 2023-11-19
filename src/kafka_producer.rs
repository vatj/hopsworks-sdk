use apache_avro::types::Record;
use apache_avro::Schema;
use color_eyre::Result;
use indicatif::ProgressBar;
use polars::frame::row::Row;
use polars::prelude::*;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::ClientConfig;
use std::time::Duration;
use tokio::task::JoinHandle;

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

fn convert_df_row_to_avro_record<'a>(
    avro_schema: &'a Schema,
    column_names: &[String],
    primary_keys: &[String],
    row: &Row<'_>,
) -> Result<(Record<'a>, String)> {
    let mut composite_key: Vec<String> = vec![];
    let mut record = Record::new(avro_schema).unwrap();

    for (jdx, value) in row.0.iter().enumerate() {
        match value.dtype() {
            DataType::Boolean => {
                record.put(&column_names[jdx], Some(value.try_extract::<i8>()? != 0))
            }
            DataType::Int8 => record.put(&column_names[jdx], Some(value.try_extract::<i32>()?)),
            DataType::Int16 => record.put(&column_names[jdx], Some(value.try_extract::<i32>()?)),
            DataType::Int32 => record.put(&column_names[jdx], Some(value.try_extract::<i32>()?)),
            DataType::Int64 => record.put(&column_names[jdx], Some(value.try_extract::<i64>()?)),
            DataType::UInt8 => record.put(
                &column_names[jdx],
                Some(value.try_extract::<u8>()? as usize),
            ),
            DataType::UInt16 => record.put(
                &column_names[jdx],
                Some(value.try_extract::<u16>()? as usize),
            ),
            DataType::UInt32 => record.put(
                &column_names[jdx],
                Some(value.try_extract::<u32>()? as usize),
            ),
            DataType::UInt64 => record.put(
                &column_names[jdx],
                Some(value.try_extract::<u64>()? as usize),
            ),
            DataType::Duration(TimeUnit::Nanoseconds) => {
                record.put(&column_names[jdx], Some(value.try_extract::<i64>()?))
            }
            DataType::Duration(TimeUnit::Microseconds) => {
                record.put(&column_names[jdx], Some(value.try_extract::<i64>()?))
            }
            DataType::Duration(TimeUnit::Milliseconds) => {
                record.put(&column_names[jdx], Some(value.try_extract::<i32>()?))
            }
            DataType::Float32 => record.put(&column_names[jdx], Some(value.try_extract::<f32>()?)),
            DataType::Float64 => record.put(&column_names[jdx], Some(value.try_extract::<f64>()?)),
            DataType::Utf8 => record.put(&column_names[jdx], Some(value.to_string())),
            DataType::Datetime(TimeUnit::Microseconds, None) => {
                record.put(&column_names[jdx], Some(value.try_extract::<i64>()?))
            }
            DataType::Datetime(TimeUnit::Nanoseconds, None) => {
                record.put(&column_names[jdx], Some(value.try_extract::<i64>()?))
            }
            DataType::Datetime(TimeUnit::Milliseconds, None) => {
                record.put(&column_names[jdx], Some(value.try_extract::<i32>()?))
            }
            DataType::Datetime(TimeUnit::Microseconds, Some(_)) => {
                return Err(color_eyre::Report::msg(
                    "Datetime with timezone not supported",
                ));
            }
            DataType::Datetime(TimeUnit::Nanoseconds, Some(_)) => {
                return Err(color_eyre::Report::msg(
                    "Datetime with timezone not supported",
                ));
            }
            DataType::Datetime(TimeUnit::Milliseconds, Some(_)) => {
                return Err(color_eyre::Report::msg(
                    "Datetime with timezone not supported",
                ));
            }
            DataType::Date => record.put(&column_names[jdx], Some(value.try_extract::<i32>()?)),
            DataType::Time => record.put(&column_names[jdx], Some(value.try_extract::<i32>()?)),
            DataType::Null => record.put(&column_names[jdx], None::<()>),
            DataType::Decimal(_, _) => todo!(),
            DataType::Binary => todo!(),
            DataType::Array(_, _) => todo!(),
            DataType::List(_) => todo!(),
            DataType::Categorical(_) => todo!(),
            DataType::Struct(_) => todo!(),
            DataType::Unknown => todo!(),
        }

        if primary_keys.contains(&column_names[jdx]) {
            composite_key.push(value.to_string())
        }
    }

    Ok((record, composite_key.join("_")))
}

fn make_future_record_from_encoded<'a>(
    the_key: &'a str,
    encoded_payload: &'a Vec<u8>,
    topic_name: &'a str,
    project_id: &str,
    feature_group_id: &str,
    subject_id: &str,
) -> Result<FutureRecord<'a, str, Vec<u8>>> {
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

// Function to produce a single Avro record
async fn produce_avro_record(
    producer: &FutureProducer,
    composite_key: &str,
    encoded_payload: Vec<u8>,
    online_topic_name: &str,
    project_id: &str,
    feature_group_id: &str,
    subject_id: &str,
) -> Result<()> {
    let produce_future = producer.send(
        make_future_record_from_encoded(
            composite_key,
            &encoded_payload,
            online_topic_name,
            project_id,
            feature_group_id,
            subject_id,
        )?,
        Duration::from_secs(5),
    );

    match produce_future.await {
        Ok(_delivery) => Ok(()),
        Err((e, _)) => Err(color_eyre::Report::msg(e.to_string())),
    }
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

    let avro_schema = Schema::parse_str(subject_dto.schema.as_str())?;

    // This value are wrapped into an Arc to allow read-only access across threads
    // meaning clone only increases the ref count, no extra-memory is allocated
    let project_id = Arc::new(
        get_hopsworks_client()
            .await
            .get_project_id()
            .lock()
            .await
            .unwrap()
            .to_string(),
    );
    let subject_id = Arc::new(subject_dto.id.to_string());
    let feature_group_id = Arc::new(feature_group_id.to_string());
    let topic_name = Arc::new(online_topic_name);

    let column_names: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    let primary_keys: Vec<String> = primary_keys.iter().map(|s| s.to_string()).collect();

    df.as_single_chunk_par();
    let mut row = df.get_row(0)?;

    let mut handles: Vec<JoinHandle<Result<()>>> = Vec::new();

    for idx in 0..df.height() {
        df.get_row_amortized(idx, &mut row)?;
        let (record, composite_key) =
            convert_df_row_to_avro_record(&avro_schema, &column_names, &primary_keys, &row)?;
        let encoded_payload = apache_avro::to_avro_datum(&avro_schema, record)?;

        let producer = producer.clone();
        let topic_name = topic_name.to_string();
        let project_id = project_id.clone();
        let feature_group_id = feature_group_id.clone();
        let subject_id = subject_id.clone();

        let handle = tokio::spawn(async move {
            produce_avro_record(
                &producer,
                &composite_key,
                encoded_payload,
                &topic_name,
                &project_id,
                &feature_group_id,
                &subject_id,
            )
            .await
        });

        handles.push(handle);
    }

    let progress_bar = ProgressBar::new(df.height() as u64);
    for handle in handles {
        progress_bar.inc(1);
        handle.await??;
    }

    producer.flush(Duration::from_secs(1))?;

    Ok(())
}
