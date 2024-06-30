use pyo3::prelude::*;
use crate::tokio;
use serde::{Serialize, Deserialize};

#[pyclass]
#[repr(transparent)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PyJobExecution {
    pub(crate) job_execution: hopsworks_api::JobExecution,
}

#[pymethods]
impl PyJobExecution {
    fn job_name(&self) -> PyResult<String> {
        Ok(self.job_execution.job_name().to_string())
    }

    fn id(&self) -> PyResult<i32> {
        Ok(self.job_execution.id())
    }

    fn state(&self) -> PyResult<String> {
        Ok(self.job_execution.state().to_string())
    }

    fn submission_time(&self) -> PyResult<String> {
        Ok(self.job_execution.submission_time().to_string())
    }

    fn get_current_state(&self) -> PyResult<String> {
        Ok(tokio().block_on(self.job_execution.get_current_state())?.to_string())
    }

    fn download_logs(&self, local_dir: Option<String>) -> PyResult<()> {
        tokio().block_on(self.job_execution.download_logs(local_dir.as_deref()))?;
        Ok(())
    }

    fn delete(&self) -> PyResult<()> {
        tokio().block_on(self.job_execution.delete())?;
        Ok(())
    }

    fn stop(&self) -> PyResult<()> {
        tokio().block_on(self.job_execution.stop())?;
        Ok(())
    }

    fn await_termination(&self) -> PyResult<()> {
        tokio().block_on(self.job_execution.await_termination())?;
        Ok(())
    }
}

impl From<hopsworks_api::JobExecution> for PyJobExecution {
    fn from(job_execution: hopsworks_api::JobExecution) -> Self {
        Self { job_execution }
    }
}

impl From<PyJobExecution> for hopsworks_api::JobExecution {
    fn from(job_execution: PyJobExecution) -> Self {
        job_execution.job_execution
    }
}