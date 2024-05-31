use color_eyre::Result;

use crate::{platform::job::Job, cluster_api::platform::job};

pub async fn get_job_by_name(job_name: &str) -> Result<Job> {
    Ok(Job::from(job::service::get_job_by_name(job_name).await?))
}

async fn validate_job_name(job_name: &str, should_exist: bool) -> Result<()> {
    // Most validation is done by Hopsworks, but we can do some basic checks here
    if job_name.is_empty() {
        return Err(color_eyre::Report::msg("job_name is empty"));
    }
    let exists = job::service::exists(job_name).await?;
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

pub async fn create_job(job_name: &str, job_config: serde_json::Value) -> Result<job::JobDTO> {
    validate_job_configuration(&job_config)?;
    validate_job_name(job_name, false).await?;
    job::service::create_job(job_name, job_config).await
}

pub async fn update_job(job_name: &str, job_config: serde_json::Value) -> Result<job::JobDTO> {
    validate_job_configuration(&job_config)?;
    validate_job_name(job_name, true).await?;
    job::service::update_job(job_name, job_config).await
}

pub async fn delete_job(job_name: &str) -> Result<()> {
    validate_job_name(job_name, true).await?;
    job::service::delete_job(job_name).await
}

pub async fn get_job_configuration(job_type: &str) -> Result<serde_json::Value> {
    job::service::get_job_configuration(job_type).await
}

pub async fn get_job_list() -> Result<Vec<Job>> {
    Ok(job::service::get_job_list()
        .await?
        .into_iter()
        .map(Job::from)
        .collect())
}
