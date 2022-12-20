use color_eyre::Result;

use crate::repositories::kafka;
use crate::repositories::kafka::entities::KafkaSubjectDTO;

pub async fn get_project_broker_endpoints(project_id: i32, external: bool) -> Result<Vec<String>> {
    let brokers_dto = kafka::service::get_project_broker_endpoints(project_id, external).await?;

    Ok(brokers_dto
        .brokers
        .iter()
        .map(|broker_str| {
            if external {
                broker_str.replace("EXTERNAL://", "")
            } else {
                broker_str.replace("INTERNAL://", "")
            }
        })
        .collect())
}

pub async fn get_kafka_topic_subject(project_id: i32, topic_name: &str) -> Result<KafkaSubjectDTO> {
    kafka::service::get_kafka_topic_subject(project_id, topic_name).await
}
