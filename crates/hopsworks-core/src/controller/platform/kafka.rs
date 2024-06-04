use color_eyre::Result;

use crate::cluster_api::platform::kafka;
use crate::platform::kafka::KafkaSubject;

pub async fn get_project_broker_endpoints(external: bool) -> Result<Vec<String>> {
    let brokers_dto = kafka::service::get_project_broker_endpoints(external).await?;

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

pub async fn get_kafka_topic_subject(
    subject_name: &str,
    opt_version: Option<&str>,
) -> Result<KafkaSubject> {
    Ok(KafkaSubject::from(kafka::service::get_kafka_topic_subject(subject_name, opt_version).await?))
}
