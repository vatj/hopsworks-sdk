use color_eyre::Result;

use crate::get_hopsworks_client;

use super::entities::{KafkaBrokersDTO, KafkaSubjectDTO};

pub async fn get_project_broker_endpoints(
    project_id: i32,
    external: bool,
) -> Result<KafkaBrokersDTO> {
    let query_params = [("external", external)];

    Ok(get_hopsworks_client()
        .await
        .send_get_with_query_params(
            format!("project/{project_id}/kafka/clusterinfo").as_str(),
            &query_params,
        )
        .await?
        .json::<KafkaBrokersDTO>()
        .await?)
}

pub async fn get_kafka_topic_subject(project_id: i32, topic_name: &str) -> Result<KafkaSubjectDTO> {
    Ok(get_hopsworks_client()
        .await
        .send_get(format!("project/{project_id}/kafka/topics/{topic_name}/subjects").as_str())
        .await?
        .json::<KafkaSubjectDTO>()
        .await?)
}
