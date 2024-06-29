use pyo3::prelude::*;
use crate::tokio;

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

#[pymethods]
impl PyJobExecution {
    fn await_termination(&self) -> PyResult<()> {
        tokio().block_on(self.job_execution.await_termination())?;
        Ok(())
    }
}