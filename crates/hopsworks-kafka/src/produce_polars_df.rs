use color_eyre::Result;
use rdkafka::producer::{FutureProducer, Producer};
use indicatif::ProgressBar;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use apache_avro::Schema;

use crate::helper::make_future_record_from_encoded;
use crate::polars_row_to_avro::convert_df_row_to_avro_record;

pub async fn produce_df(
    headers: rdkafka::message::OwnedHeaders,
    topic_name: Arc<String>,
    primary_keys: &[&str],
    avro_schema: Schema,
    producer: &FutureProducer,
    df: &mut polars::prelude::DataFrame,
) -> Result<()> {
    df.as_single_chunk_par();

    let column_names: Vec<&str> = df.get_column_names();
    let mut row = df.get_row(0)?;

    let mut handles: Vec<JoinHandle<Result<()>>> = Vec::new();

    for idx in 0..df.height() {
        df.get_row_amortized(idx, &mut row)?;
        let (record, composite_key) =
            convert_df_row_to_avro_record(&avro_schema, &column_names, primary_keys, &row)?;
        let encoded_payload = apache_avro::to_avro_datum(&avro_schema, record)?;

        // // Need to clone the values to move them into the spawned thread
        // // Thanks to Arc no data should be copied
        let producer = producer.clone();
        let topic_name = topic_name.clone();
        let headers = headers.clone();

        let handle = tokio::spawn(async move {
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