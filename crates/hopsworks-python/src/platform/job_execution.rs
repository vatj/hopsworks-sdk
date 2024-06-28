use pyo3::prelude::*;

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct PyJobExecution {
    pub(crate) job_execution: hopsworks_api::JobExecution,
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
