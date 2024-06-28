use pyo3::prelude::*;

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct PyJob {
    pub(crate) job: hopsworks_api::Job,
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
