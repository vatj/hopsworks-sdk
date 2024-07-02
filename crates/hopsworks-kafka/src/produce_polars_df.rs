use color_eyre::Result;
use polars::prelude::*;
use polars_arrow::io::avro::write::BoxSerializer;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use indicatif::ProgressBar;
use std::sync::Arc;
use std::time::Duration;
use std::vec;
use tokio::task::JoinHandle;
use apache_avro::Schema;

use crate::helper::make_future_record_from_encoded;
use crate::polars_row_to_avro::convert_df_row_to_avro_record;

pub async fn produce_df(
    headers: rdkafka::message::OwnedHeaders,
    topic_name: Arc<String>,
    primary_keys: Vec<&str>,
    avro_schema: Schema,
    producer: &FutureProducer,
    df: &mut polars::prelude::DataFrame,
) -> Result<()> {
    df.as_single_chunk_par();
    
    let schema = df.schema().to_arrow(false);
    let record = polars_arrow::io::avro::write::to_record(&schema, "".to_string())?;

    let primary_keys: Vec<String> = primary_keys.iter().map(|s| s.to_string()).collect();
    let column_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();

    let column_ref: Arc<Vec<String>> = Arc::new(column_names);
    let primary_ref: Arc<Vec<String>> = Arc::new(primary_keys);
    let schema_ref = Arc::new(avro_schema);

    let mut handles: Vec<JoinHandle<Result<()>>> = Vec::new();

    for chunk in df.iter_chunks(false, true) {
        let mut serializers = chunk
                .iter()
                .zip(record.fields.iter())
                .map(|(array, field)| polars_arrow::io::avro::write::new_serializer(array.as_ref(), &field.schema))
                .collect::<Vec<_>>();

        
        
        // Need to clone the values to move them into the spawned thread
        // Thanks to Arc no data should be copied
        let producer = producer.clone();
        let topic_name = topic_name.clone();
        let headers = headers.clone();
        let schema_ref = schema_ref.clone();
        let column_ref = column_ref.clone();
        let primary_ref = primary_ref.clone();

        let handle = tokio::spawn(async move {
            let (record, composite_key) =
                convert_df_row_to_avro_record(&schema_ref, &column_ref, &primary_ref, &row)?;
            let encoded_payload = apache_avro::to_avro_datum(&schema_ref, record)?;

            let produce_future = producer.send(
                make_future_record_from_encoded(
                    &composite_key,
                    &encoded_payload,
                    &topic_name,
                    headers,
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

pub fn serialize_to_future<T>(key_serializers: &mut T, value_serializers: &mut [BoxSerializer], producer: FutureProducer) -> Result<()> where T: Iterator<Item = String> {
    let mut data: Vec<u8> = vec![];
    let number_of_rows = 1;
    let produced_futures = Vec::with_capacity(number_of_rows);

    // _the_ transpose (columns -> rows)
    for _ in 0..number_of_rows {
        for serializer in &mut *value_serializers {
            let item_data = serializer.next().unwrap();
            data.extend(item_data);
        }
        let composite_key = key_serializers.next().unwrap(); 

        produced_futures.push(
            producer.send(
            make_future_record_from_encoded(&composite_key, &data, topic_name, headers)?,
            Duration::from_secs(5),
        )
    );

    }

    Ok(())
    
}