use color_eyre::Result;

use crate::cluster_api::platform::job::service;
use crate::platform::job::Job;

pub async fn get_job_by_name(job_name: &str) -> Result<Job> {
    Ok(Job::from(service::get_job_by_name(job_name).await?))
}

async fn validate_job_name(job_name: &str, should_exist: bool) -> Result<()> {
    // Most validation is done by Hopsworks, but we can do some basic checks here
    if job_name.is_empty() {
        return Err(color_eyre::Report::msg("job_name is empty"));
    }
    let exists = service::exists(job_name).await?;
    if should_exist && !exists {
        return Err(color_eyre::Report::msg(format!(
            "job with name {} does not exist",
            job_name
        )));
    } else if !should_exist && exists {
        return Err(color_eyre::Report::msg(format!(
            "job with name {} already exists",
            job_name
        )));
    }

    Ok(())
}

fn validate_job_configuration(job_config: &serde_json::Value) -> Result<()> {
    job_config
        .get("job_type")
        .ok_or_else(|| color_eyre::Report::msg("job_type is missing from job configuration"))?;

    Ok(())
}

pub async fn create_job(job_name: &str, job_config: serde_json::Value) -> Result<Job> {
    validate_job_configuration(&job_config)?;
    validate_job_name(job_name, false).await?;
    Ok(Job::from(service::create_job(job_name, job_config).await?))
}

pub async fn update_job(job_name: &str, job_config: serde_json::Value) -> Result<Job> {
    validate_job_configuration(&job_config)?;
    validate_job_name(job_name, true).await?;
    Ok(Job::from(service::update_job(job_name, job_config).await?))
}

pub async fn delete_job(job_name: &str) -> Result<()> {
    validate_job_name(job_name, true).await?;
    service::delete_job(job_name).await
}

pub async fn get_job_configuration(job_type: &str) -> Result<serde_json::Value> {
    service::get_job_configuration(job_type).await
}

pub async fn get_job_list() -> Result<Vec<Job>> {
    Ok(service::get_job_list()
        .await?
        .iter()
        .map(|job_dto| Job::from(job_dto.to_owned()))
        .collect())
}
