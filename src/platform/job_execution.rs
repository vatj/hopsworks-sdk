use color_eyre::Result;
use serde::{Deserialize, Serialize};

use crate::repositories::platform::job_execution::JobExecutionDTO;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Execution {
    href: String,
    id: i32,
}

impl Execution {
    fn new_from_dto(execution_dto: JobExecutionDTO) -> Self {
        Self {
            href: execution_dto.href,
            id: execution_dto.id,
        }
    }
}

impl From<JobExecutionDTO> for Execution {
    fn from(execution_dto: JobExecutionDTO) -> Self {
        Self::new_from_dto(execution_dto)
    }
}

impl Execution {
    pub async fn download_logs(&self) -> Result<()> {
        crate::core::platform::job_execution::download_logs().await
    }

    pub async fn delete(&self) -> Result<()> {
        crate::core::platform::job_execution::delete().await
    }
}
