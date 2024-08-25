use color_eyre::Result;
use hopsworks_core::get_threaded_runtime_num_worker_threads;
use polars::lazy::dsl::Expr;
use polars::prelude::*;
use polars_arrow::io::avro::avro_schema::schema::Record;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::ClientConfig;
use std::sync::Arc;
use std::time::Duration;
use std::vec;

#[tracing::instrument(
    skip(df, producer_config))]
pub async fn produce_df(
    headers: rdkafka::message::OwnedHeaders,
    topic_name: Arc<String>,
    primary_keys: Vec<&str>,
    producer_config: ClientConfig,
    df: &mut polars::prelude::DataFrame,
) -> Result<()> {
    tracing::debug!("Rechunks to a single chunk");
    df.as_single_chunk_par();
    
    let the_start_time = std::time::Instant::now();
    let mut join_set_workers: tokio::task::JoinSet<Result<usize>> = tokio::task::JoinSet::new();
    let schema = df.schema().to_arrow(false);
    let record = Arc::new(polars_arrow::io::avro::write::to_record(&schema, "".to_string())?);
    let multi_producer = std::env::var("HOPSWORKS_KAFKA_MULTI_PRODUCER").unwrap_or("false".to_string()) == "true";
    
    let pk_series_expr = pk_series_lazy_expr(schema, primary_keys)?;
    let dfs = df
        .clone()
        .lazy()
        .with_column(pk_series_expr)
        .collect()?
        .split_chunks_by_n(get_threaded_runtime_num_worker_threads(), true);

    let producer_0: FutureProducer = producer_config.clone().create()?;
    for (idx, frame) in dfs.iter().enumerate() {
        let producer = if multi_producer || idx != 0 {
            producer_config.clone().create()?
        } else {
            producer_0.clone()
        };
        let topic_name = topic_name.clone();
        let headers = headers.clone();
        let record = record.clone();

        let (tx, rx) = tokio::sync::oneshot::channel::<DataFrame>();
        tx.send(frame.clone()).map_err(|e| color_eyre::Report::msg(e.to_string()))?;
        
        join_set_workers.build_task().name(format!("hopsworks_insert_worker_{idx}").as_str()).spawn(async move {
            serialize_and_produce_chunk(idx, record, producer, rx, topic_name, headers).await
        })?;
    }
    while let Some(res) = join_set_workers.join_next().await {
        let idx = res??;
        tracing::debug!("Closing hopsworks_insert_worker_{} in {:?}", idx, the_start_time.elapsed());
    }

    Ok(())
}

use std::ops::Add;

#[tracing::instrument]
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
    
    let mut sum_expr: Expr = polars_expr.pop().unwrap();
    while let Some(col_expr) = polars_expr.pop() {
        sum_expr = sum_expr.add(col_expr);
    }
    tracing::debug!("polars Expr to compute primary keys string columns: {:?}", sum_expr);

    Ok(sum_expr.alias("hopsworks_pk"))
}

#[tracing::instrument(
    skip(producer, rx)
)]
async fn serialize_and_produce_chunk(
    idx: usize,
    record: Arc<Record>,
    producer: FutureProducer,
    rx: tokio::sync::oneshot::Receiver<DataFrame>,
    topic_name: Arc<String>,
    headers: rdkafka::message::OwnedHeaders,
) -> Result<usize> {
    tracing::debug!("Processing chunk: {}", idx);
    let frame: DataFrame = rx.await?;
    let mut produced_handles = tokio::task::JoinSet::new();
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

        let producer = producer.clone();
        let topic_name = topic_name.clone();
        let headers = headers.clone();

        produced_handles.spawn(async move {
            let produce_future = producer.send(
                FutureRecord::to(topic_name.as_str())
                    .payload(&data)
                    .key(&composite_key)
                    .headers(headers),
                Duration::from_secs(120),
            );

            match produce_future.await {
                Ok(_delivery) => Ok(()),
                Err((e, _)) => Err(color_eyre::Report::msg(e.to_string())),
            }
        });
    }
    tracing::debug!("Serialized chunk {} with {} in {:?}", idx,  chunk.len(), start_time.elapsed());
    while let Some(res) = produced_handles.join_next().await {
        res??;
    }
    tracing::debug!("Flushing producer after {:?}", start_time.elapsed());
    producer.flush(Duration::from_secs(120))?;
    tracing::debug!("Produced chunk {} in {:?} ", idx, start_time.elapsed());
    Ok(idx)
}