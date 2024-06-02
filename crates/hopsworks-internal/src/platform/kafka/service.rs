use color_eyre::Result;
use reqwest::Method;

use crate::get_hopsworks_client;

use super::{KafkaBrokersDTO, KafkaSubjectDTO};

pub async fn get_project_broker_endpoints(external: bool) -> Result<KafkaBrokersDTO> {
    let query_params = [("external", external)];

    Ok(get_hopsworks_client()
        .await
        .request(Method::GET, "kafka/clusterinfo", true, true)
        .await?
        .query(query_params.as_ref())
        .send()
        .await?
        .json::<KafkaBrokersDTO>()
        .await?)
}

pub async fn get_kafka_topic_subject(
    subject_name: &str,
    opt_version: Option<&str>,
) -> Result<KafkaSubjectDTO> {
    let version = opt_version.unwrap_or("latest");
    Ok(get_hopsworks_client()
        .await
        .request(
            Method::GET,
            format!("kafka/subjects/{subject_name}/versions/{version}").as_str(),
            true,
            true,
        )
        .await?
        .send()
        .await?
        .json::<KafkaSubjectDTO>()
        .await?)
}
