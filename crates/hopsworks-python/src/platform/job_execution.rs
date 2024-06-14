use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct JobExecution {
    pub(crate) job_execution: hopsworks_api::JobExecution,
}

impl From<hopsworks_api::JobExecution> for JobExecution {
    fn from(job_execution: hopsworks_api::JobExecution) -> Self {
        Self { job_execution }
    }
}

impl From<JobExecution> for hopsworks_api::JobExecution {
    fn from(job_execution: JobExecution) -> Self {
        job_execution.job_execution
    }
}
