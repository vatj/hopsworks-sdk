
use rdkafka::{consumer::{BaseConsumer, Consumer}, message::{Header, OwnedHeaders}, metadata, ClientConfig};
use color_eyre::Result;

pub(crate) fn make_custom_headers(
    project_id: i32,
    feature_group_id: i32,
    subject_id: i32,
    version: i32,
) -> OwnedHeaders {
    OwnedHeaders::new()
        .insert(Header {
            key: "version",
            value: Some(&version.to_string()),
        })
        .insert(Header {
            key: "projectId",
            value: Some(&project_id.to_string()),
        })
        .insert(Header {
            key: "featureGroupId",
            value: Some(&feature_group_id.to_string()),
        })
        .insert(Header {
            key: "subjectId",
            value: Some(&subject_id.to_string()),
        })
}

pub(crate) fn get_kafka_offsets(
    kafka_config: ClientConfig,
    topic_name: &str,
    high: bool,
) -> Result<String> {
    let mut offsets: Vec<String> = Vec::with_capacity(10);
    let timeout = std::time::Duration::from_secs(10);
    let kafka_consumer : BaseConsumer = kafka_config.create()?;
    let metadata = kafka_consumer
        .client()
        .fetch_metadata(Some(topic_name), timeout)?;

    if let Some(topic_metadata) = metadata.topics().iter().find(|t| t.name() == topic_name) {
        let partitions = topic_metadata.partitions();
        for partition in partitions {
            let (low_watermark, high_watermark) = kafka_consumer.fetch_watermarks(topic_name, partition.id(), timeout)?;
            offsets.push(format!("{}:{}", partition.id(), if high { high_watermark } else { low_watermark }));
        };
    }

    Ok(offsets.join(","))
}