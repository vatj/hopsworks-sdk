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

use hopsworks_internal::get_hopsworks_client;
use hopsworks_core::core::platform::kafka::get_kafka_topic_subject;
use hopsworks_core::hopsworks_internal::feature_store::storage_connector::entities::FeatureStoreKafkaConnectorDTO;
use hopsworks_core::hopsworks_internal::platform::kafka::entities::KafkaSubjectDTO;

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
    column_names: &[&str],
    primary_keys: &[&str],
    row: &Row<'_>,
) -> Result<(Record<'a>, String)> {
    let mut composite_key: Vec<String> = vec![];
    let mut record = Record::new(avro_schema).unwrap();

    for (jdx, value) in row.0.iter().enumerate() {
        match value.dtype() {
            DataType::Boolean => {
                record.put(column_names[jdx], Some(value.try_extract::<i8>()? != 0))
            }
            DataType::Int8 => record.put(column_names[jdx], Some(value.try_extract::<i32>()?)),
            DataType::Int16 => record.put(column_names[jdx], Some(value.try_extract::<i32>()?)),
            DataType::Int32 => record.put(column_names[jdx], Some(value.try_extract::<i32>()?)),
            DataType::Int64 => record.put(column_names[jdx], Some(value.try_extract::<i64>()?)),
            DataType::UInt8 => {
                record.put(column_names[jdx], Some(value.try_extract::<u8>()? as usize))
            }
            DataType::UInt16 => record.put(
                column_names[jdx],
                Some(value.try_extract::<u16>()? as usize),
            ),
            DataType::UInt32 => record.put(
                column_names[jdx],
                Some(value.try_extract::<u32>()? as usize),
            ),
            DataType::UInt64 => record.put(
                column_names[jdx],
                Some(value.try_extract::<u64>()? as usize),
            ),
            DataType::Duration(TimeUnit::Nanoseconds) => {
                record.put(column_names[jdx], Some(value.try_extract::<i64>()?))
            }
            DataType::Duration(TimeUnit::Microseconds) => {
                record.put(column_names[jdx], Some(value.try_extract::<i64>()?))
            }
            DataType::Duration(TimeUnit::Milliseconds) => {
                record.put(column_names[jdx], Some(value.try_extract::<i32>()?))
            }
            DataType::Float32 => record.put(column_names[jdx], Some(value.try_extract::<f32>()?)),
            DataType::Float64 => record.put(column_names[jdx], Some(value.try_extract::<f64>()?)),
            DataType::Utf8 => record.put(column_names[jdx], Some(value.to_string())),
            DataType::Datetime(TimeUnit::Microseconds, None) => {
                record.put(column_names[jdx], Some(value.try_extract::<i64>()?))
            }
            DataType::Datetime(TimeUnit::Nanoseconds, None) => {
                record.put(column_names[jdx], Some(value.try_extract::<i64>()?))
            }
            DataType::Datetime(TimeUnit::Milliseconds, None) => {
                record.put(column_names[jdx], Some(value.try_extract::<i32>()?))
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
            DataType::Date => record.put(column_names[jdx], Some(value.try_extract::<i32>()?)),
            DataType::Time => record.put(column_names[jdx], Some(value.try_extract::<i32>()?)),
            DataType::Null => record.put(column_names[jdx], None::<()>),
            DataType::Decimal(_, _) => todo!(),
            DataType::Binary => todo!(),
            DataType::Array(_, _) => todo!(),
            DataType::List(_) => todo!(),
            DataType::Categorical(_, _) => todo!(),
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
    composite_key: &'a str,
    encoded_payload: &'a Vec<u8>,
    topic_name: &'a str,
    project_id: &str,
    feature_group_id: &str,
    subject_id: &str,
    version: &str,
) -> Result<FutureRecord<'a, str, Vec<u8>>> {
    Ok(FutureRecord::to(topic_name)
        .payload(encoded_payload)
        .key(composite_key)
        .headers(
            OwnedHeaders::new()
                .insert(Header {
                    key: "version",
                    value: Some(version),
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

pub async fn produce_df(
    df: &mut polars::prelude::DataFrame,
    kafka_connector: FeatureStoreKafkaConnectorDTO,
    subject_name: &str,
    opt_version: Option<&str>,
    online_topic_name: &str,
    primary_keys: &[&str],
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
    let version = Arc::new(subject_dto.version.to_string());

    df.as_single_chunk_par();

    let column_names: Vec<&str> = df.get_column_names();
    let mut row = df.get_row(0)?;

    let mut handles: Vec<JoinHandle<Result<()>>> = Vec::new();

    for idx in 0..df.height() {
        df.get_row_amortized(idx, &mut row)?;
        let (record, composite_key) =
            convert_df_row_to_avro_record(&avro_schema, &column_names, primary_keys, &row)?;
        let encoded_payload = apache_avro::to_avro_datum(&avro_schema, record)?;

        // Need to clone the values to move them into the spawned thread
        // Thanks to Arc no data should be copied
        let producer = producer.clone();
        let topic_name = topic_name.to_string();
        let project_id = project_id.clone();
        let feature_group_id = feature_group_id.clone();
        let subject_id = subject_id.clone();
        let version = version.clone();

        let handle = tokio::spawn(async move {
            let produce_future = producer.send(
                make_future_record_from_encoded(
                    &composite_key,
                    &encoded_payload,
                    &topic_name,
                    &project_id,
                    &feature_group_id,
                    &subject_id,
                    &version,
                )?,
                Duration::from_secs(5),
            );

            match produce_future.await {
                Ok(_delivery) => Ok(()),
                Err((e, _)) => Err(color_eyre::Report::msg(e.to_string())),
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::Schema;
    use rdkafka::message::Headers;

    #[tokio::test]
    async fn test_convert_df_row_to_avro_record() {
        // Define your Avro schema based on the expected structure
        let avro_schema = Schema::parse_str(
            "{
                \"type\" : \"record\",
                \"namespace\" : \"df\",
                \"name\" : \"fg\",
                \"fields\" : [
                    { \"name\" : \"i8\" , \"type\" : \"int\" },
                    { \"name\" : \"i16\" , \"type\" : \"int\" },
                    { \"name\" : \"i32\", \"type\": \"int\" },
                    { \"name\" : \"i64\" , \"type\" : \"long\" },
                    { \"name\" : \"u8\" , \"type\" : \"int\" },
                    { \"name\" : \"u16\", \"type\": \"int\" },
                    { \"name\" : \"u32\" , \"type\" : \"int\" },
                    { \"name\" : \"u64\", \"type\": \"long\" },
                    { \"name\" : \"f32\" , \"type\" : \"float\" },
                    { \"name\" : \"f64\" , \"type\" : \"double\" },
                    { \"name\" : \"utf8\" , \"type\" : \"string\" },
                    { \"name\" : \"bool\" , \"type\" : \"boolean\" },
                    { \"name\" : \"date\" , \"type\" : { \"type\" : \"int\", \"logicalType\" : \"date\" } },
                    { \"name\" : \"time\" , \"type\" : { \"type\" : \"int\", \"logicalType\" : \"time-millis\" } },
                    { \"name\" : \"datetime\" , \"type\" : { \"type\" : \"long\", \"logicalType\" : \"timestamp-micros\" } },
                    { \"name\" : \"duration\" , \"type\" : { \"type\" : \"fixed\", \"size\":12, \"name\": \"DataBlockDuration\" } }
                ]
            }",
        )
        .unwrap();

        let col_names = vec![
            "i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64", "utf8", "bool",
            "date", "time", "datetime", "duration",
        ];
        let primary_keys = vec!["i64", "utf8"];
        // Create a sample Polars DataFrame row
        let mut df = DataFrame::new(vec![
            Series::new(col_names[0], &[1i8]),
            Series::new(col_names[1], &[1i16]),
            Series::new(col_names[2], &[1i32]),
            Series::new(col_names[3], &[1i64]),
            Series::new(col_names[4], &[1u8]),
            Series::new(col_names[5], &[1u16]),
            Series::new(col_names[6], &[1u32]),
            Series::new(col_names[7], &[1u64]),
            Series::new(col_names[8], &[1f32]),
            Series::new(col_names[9], &[1f64]),
            Series::new(col_names[10], &["test"]),
            Series::new(col_names[11], &[true]),
            Series::new(col_names[12], &[1i32])
                .cast(&DataType::Date)
                .unwrap(),
            Series::new(col_names[13], &[1i32])
                .cast(&DataType::Time)
                .unwrap(),
            Series::new(col_names[14], &[1i64])
                .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
                .unwrap(),
            Series::new(col_names[15], &[1i64])
                .cast(&DataType::Duration(TimeUnit::Microseconds))
                .unwrap(),
        ])
        .unwrap();

        df.as_single_chunk();

        let row = df.get_row(0).unwrap();
        let result = convert_df_row_to_avro_record(&avro_schema, &col_names, &primary_keys, &row);

        assert!(result.is_ok());

        let (avro_record, composite_key) = result.unwrap();

        assert_eq!(composite_key, "1_\"test\"");

        // Assert specific values in the Avro record based on your expectations
        for name in col_names {
            assert!(avro_record.fields.iter().any(|field| field.0 == name));
        }
    }

    #[tokio::test]
    async fn test_make_future_record_from_encoded() {
        // Arrange
        let composite_key = "some_key";
        let encoded_payload = vec![1, 2, 3];
        let topic_name = "test_topic";
        let project_id = "project123";
        let feature_group_id = "feature_group_1";
        let subject_id = "subject_1";
        let version = "1";

        // Act
        let result = make_future_record_from_encoded(
            composite_key,
            &encoded_payload,
            topic_name,
            project_id,
            feature_group_id,
            subject_id,
            version,
        );

        // Assert
        assert!(result.is_ok());

        let future_record = result.unwrap();

        assert_eq!(future_record.key, Some(composite_key));
        assert_eq!(future_record.payload, Some(&encoded_payload));
        assert_eq!(future_record.topic, topic_name);

        assert!(future_record.headers.is_some());
        future_record
            .headers
            .unwrap()
            .iter()
            .for_each(|header| match header.key {
                "version" => assert_eq!(header.value, Some(version.as_bytes())),
                "projectId" => assert_eq!(header.value, Some(project_id.as_bytes())),
                "featureGroupId" => assert_eq!(header.value, Some(feature_group_id.as_bytes())),
                "subjectId" => assert_eq!(header.value, Some(subject_id.as_bytes())),
                _ => panic!("Unexpected header"),
            });
    }

    #[tokio::test]
    async fn test_setup_future_producer() {
        // Arrange
        let _ = crate::login(None).await.unwrap();
        let kafka_connector = FeatureStoreKafkaConnectorDTO {
            bootstrap_servers: "localhost:9092".to_string(),
            _type: "kafka".to_string(),
            security_protocol: "ssl".to_string(),
            ssl_endpoint_identification_algorithm: "none".to_string(),
            options: vec![],
            external_kafka: true,
            id: 1,
            description: "empty".to_string(),
            name: "kafka_connector_1".to_string(),
            featurestore_id: 1,
            storage_connector_type: "kafka".to_string(),
        };

        // Act
        let result = setup_future_producer(kafka_connector).await;

        // Assert
        assert!(result.is_ok());
    }
}
