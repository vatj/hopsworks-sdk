use color_eyre::Result;
use log::debug;
use polars::lazy::dsl::Expr;
use polars::prelude::*;
use rdkafka::producer::{FutureProducer, Producer};
use indicatif::ProgressBar;
use tokio::task::JoinSet;
use std::sync::Arc;
use std::time::Duration;
use std::vec;

use crate::helper::make_future_record_from_encoded;

pub async fn produce_df(
    headers: rdkafka::message::OwnedHeaders,
    topic_name: Arc<String>,
    primary_keys: Vec<&str>,
    producer: &FutureProducer,
    df: &mut polars::prelude::DataFrame,
) -> Result<()> {
    df.as_single_chunk_par();
    
    // let mut join_set = tokio::task::JoinSet::new();
    let schema = df.schema().to_arrow(false);
    let record = polars_arrow::io::avro::write::to_record(&schema, "".to_string())?;

    // let pk_series_expr = pk_series_lazy_expr(schema, primary_keys)?;
    // let pk_df = chunk.clone().lazy().select(&[pk_series_expr]).collect()?;
    // let mut pk_iter = pk_df.column("hopsworks_pk")?.str()?.iter();
    
    let mut tasks: JoinSet<Result<()>> = tokio::task::JoinSet::new();
    let progress_bar = Arc::new(ProgressBar::new(df.height() as u64));

    for (idx, chunk) in  df.iter_chunks(false, true).enumerate() {
        debug!("Processing chunk: {}", idx);
        let progress_bar = progress_bar.clone();
        let producer = producer.clone();
        let topic_name = topic_name.clone();
        let headers = headers.clone();
        let record = record.clone();

        tasks.spawn(async move {
            let mut join_set = tokio::task::JoinSet::new();

            let mut serializers = chunk
            .iter()
            .zip(record.fields.iter())
            .map(|(array, field)| polars_arrow::io::avro::write::new_serializer(array.as_ref(), &field.schema))
            .collect::<Vec<_>>();


            for _ in 0..chunk.len() {
                let mut data: Vec<u8> = vec![];
                // let composite_key = pk_iter.next().unwrap().unwrap().to_string();
                let composite_key = "bob";
                for serializer in &mut *serializers {
                    let item_data = serializer.next().unwrap();
                    data.extend(item_data);
                }

                // Need to clone the values to move them into the spawned thread
                // Thanks to Arc no data should be copied
                let producer = producer.clone();
                let topic_name = topic_name.clone();
                let headers = headers.clone();

                join_set.spawn(async move {
            
                let produce_future = producer.send(
                make_future_record_from_encoded(
                        composite_key,
                        &data,
                        &topic_name,
                        headers,
                    ),
                    Duration::from_secs(5),
                );

                match produce_future.await {
                    Ok(_delivery) => Ok(()),
                    Err((e, _)) => Err(color_eyre::Report::msg(e.to_string())),
                }
            });            
        }

        while let Some(res) = join_set.join_next().await {
            res??;
            progress_bar.inc(1);
        }

        Ok(())
        
        });
    }
    let mut count = 0;
    while let Some(res) = tasks.join_next().await {
        res??;
        count += 1;
        debug!("Chunk processed : {}", count);
    }

    producer.flush(Duration::from_secs(5))?;

    Ok(())
}

use std::ops::Add;

fn pk_series_lazy_expr(schema: ArrowSchema, primary_keys: Vec<&str>) -> Result<Expr> {
    let mut polars_expr: Vec<Expr> = schema.fields
        .iter()
        .filter(|field| primary_keys.contains(&field.name.as_str()))
        .map(|field| match field.data_type() {
            ArrowDataType::LargeUtf8 => col(&field.name),
            ArrowDataType::Utf8 => col(&field.name),
            ArrowDataType::Utf8View => col(&field.name),
            _ => col(&field.name).cast(DataType::String),
        })
        .collect::<Vec<_>>();

    debug!("polars_expr: {:?}", polars_expr);
    
    let mut sum_expr: Expr = polars_expr.pop().unwrap();
    while let Some(col_expr) = polars_expr.pop() {
        sum_expr = sum_expr.add(col_expr);
    }
    Ok(sum_expr.alias("hopsworks_pk"))
}