use color_eyre::Result;
use hopsworks_core::get_threaded_runtime_num_worker_threads;
use log::debug;
use polars::lazy::dsl::Expr;
use polars::prelude::*;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::ClientConfig;
use std::sync::Arc;
use std::time::Duration;
use std::vec;

pub async fn produce_df(
    headers: rdkafka::message::OwnedHeaders,
    topic_name: Arc<String>,
    primary_keys: Vec<&str>,
    producer: ClientConfig,
    df: &mut polars::prelude::DataFrame,
) -> Result<()> {
    tracing::debug!("Rechunks to a single chunk");
    df.as_single_chunk_par();
    
    let the_start_time = std::time::Instant::now();
    let mut join_set_serializer: tokio::task::JoinSet<Result<usize>> = tokio::task::JoinSet::new();
    let schema = df.schema().to_arrow(false);
    let record = Arc::new(polars_arrow::io::avro::write::to_record(&schema, "".to_string())?);
    
    let pk_series_expr = pk_series_lazy_expr(schema, primary_keys)?;
    let dfs = df
        .clone()
        .lazy()
        .with_column(pk_series_expr)
        .collect()?
        .split_chunks_by_n(get_threaded_runtime_num_worker_threads(), true);

    let (trx, mut recx) = tokio::sync::mpsc::channel::<(String,Vec<u8>)>(1000000);
    
    let handle: tokio::task::JoinHandle<Result<()>> = tokio::task::Builder::new().name("producer thread").spawn(async move {
        let producer: FutureProducer = producer.create()?;
        tracing::debug!("Producer created");
        let mut produced_handles = tokio::task::JoinSet::new();
        let mut counter = 0;
        // necessary to wait for the serializer to start
        tokio::time::sleep(Duration::from_millis(3000)).await;
        let start_time = std::time::Instant::now();

        while let Some((composite_key, data)) = recx.recv().await {
            let producer = producer.clone();
            let topic_name = topic_name.clone();
            let headers = headers.clone();

            produced_handles.spawn(async move {
                let produce_future = producer.send(
                    FutureRecord::to(topic_name.as_str())
                        .payload(&data)
                        .key(&composite_key)
                        .headers(headers),
                    Duration::from_secs(5),
                );

                match produce_future.await {
                    Ok(_delivery) => Ok(()),
                    Err((e, _)) => Err(color_eyre::Report::msg(e.to_string())),
                }
            });
            counter += 1;
        }
        while let Some(res) = produced_handles.join_next().await {
            res??;
        }
        tracing::debug!("Flushing producer after {:?}", start_time.elapsed());
        producer.flush(Duration::from_secs(120))?;
        tracing::debug!("Produced {} rows {:?} ", counter, start_time.elapsed());

        Ok(())
    })?;
   

    for (idx, frame) in dfs.iter().enumerate() {
        let record = record.clone();
        let trx = trx.clone();

        let (tx, rx) = tokio::sync::oneshot::channel::<DataFrame>();
        tx.send(frame.clone()).unwrap();
        
        join_set_serializer.build_task().name(format!("serialize_chunk_{idx}").as_str()).spawn_blocking(move || {
            tracing::debug!("Processing chunk: {}", idx);
            let frame: DataFrame = rx.blocking_recv()?;
            let start_time = std::time::Instant::now();
            let chunk = frame
                .select(
                    frame
                    .get_column_names()
                    .into_iter()
                    .filter(|name| name.eq(&"hopsworks_pk"))
                )
                .unwrap()
                .iter_chunks(false, true)
                .next()
                .unwrap();
            
            let mut serializers = chunk
                .iter()
                .zip(record.fields.iter())
                .map(|(array, field)| polars_arrow::io::avro::write::new_serializer(array.as_ref(), &field.schema))
                .collect::<Vec<_>>();
            let mut pk_iter = frame.column("hopsworks_pk")?.str()?.iter();

            for _ in 0..chunk.len() {
                let mut data: Vec<u8> = vec![];
                let composite_key = pk_iter.next().unwrap().unwrap().to_string();
                for serializer in &mut *serializers {
                    let item_data = serializer.next().unwrap();
                    data.extend(item_data);
                }
                trx.blocking_send((composite_key, data))?;
            }
            drop(trx);
            tracing::debug!("Serialized chunk {} with {} in {:?}", idx,  chunk.len(), start_time.elapsed());
            Ok(idx)
        })?;
    }
    while let Some(res) = join_set_serializer.join_next().await {
        let idx = res??;
        tracing::debug!("Closing serializer thread {} in {:?}", idx, the_start_time.elapsed());
    }
    drop(trx);
    handle.await??;
    tracing::debug!("Closing producer after {:?}", the_start_time.elapsed());

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