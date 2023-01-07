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

pub async fn get_kafka_topic_subject(topic_name: &str) -> Result<KafkaSubjectDTO> {
    Ok(get_hopsworks_client()
        .await
        .send_get(format!("kafka/topics/{topic_name}/subjects").as_str(), true)
        .await?
        .json::<KafkaSubjectDTO>()
        .await?)
}
