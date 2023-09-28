use color_eyre::Result;

use crate::get_hopsworks_client;

use super::entities::{KafkaBrokersDTO, KafkaSubjectDTO};

pub async fn get_project_broker_endpoints(external: bool) -> Result<KafkaBrokersDTO> {
    let query_params = [("external", external)];

    Ok(get_hopsworks_client()
        .await
        .send_get_with_query_params("kafka/clusterinfo", &query_params, true)
        .await?
        .json::<KafkaBrokersDTO>()
        .await?)
}

pub async fn get_kafka_topic_subject(subject_name: &str, opt_version: Option<&str>) -> Result<KafkaSubjectDTO> {
    let version = opt_version.unwrap_or("latest");
    Ok(get_hopsworks_client()
        .await
        .send_get(format!("kafka/subjects/{subject_name}/versions/{version}").as_str(), true)
        .await?
        .json::<KafkaSubjectDTO>()
        .await?)
}
