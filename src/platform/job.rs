use color_eyre::Result;
use serde::{Deserialize, Serialize};

use crate::{core::platform::job::run_job_with_name, repositories::platform::job::JobDTO};

use super::job_execution::Execution;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Job {
    href: String,
    id: i32,
    name: String,
    creation_time: String,
    job_type: String,
}

impl Job {
    fn new_from_dto(job_dto: JobDTO) -> Self {
        Self {
            href: job_dto.href,
            id: job_dto.id,
            name: job_dto.name,
            creation_time: job_dto.creation_time,
            job_type: job_dto.job_type,
        }
    }
}

impl From<JobDTO> for Job {
    fn from(job_dto: JobDTO) -> Self {
        Self::new_from_dto(job_dto)
    }
}

impl Job {
    pub async fn run(&self) -> Result<Execution> {
        Ok(Execution::from(
            run_job_with_name(self.name.as_str()).await?,
        ))
    }

    pub async fn get_executions(&self) -> Result<Execution> {
        todo!()
    }

    pub async fn save(&self) -> Result<()> {
        todo!()
    }

    pub async fn delete(&self) -> Result<()> {
        todo!()
    }
}
