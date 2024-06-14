use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct Job {
    pub(crate) job: hopsworks_api::Job,
}


impl From<hopsworks_api::Job> for Job {
    fn from(job: hopsworks_api::Job) -> Self {
        Self { job }
    }
}

impl From<Job> for hopsworks_api::Job {
    fn from(job: Job) -> Self {
        job.job
    }
}
