use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[pyclass]
#[repr(transparent)]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PyJob {
    pub(crate) job: hopsworks_api::Job,
}

#[pymethods]
impl PyJob {
    fn name(&self) -> String {
        self.job.name().to_string()
    }
}

impl From<hopsworks_api::Job> for PyJob {
    fn from(job: hopsworks_api::Job) -> Self {
        Self { job }
    }
}

impl From<PyJob> for hopsworks_api::Job {
    fn from(job: PyJob) -> Self {
        job.job
    }
}
