use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct Project {
    pub(crate) project: hopsworks_api::Project,
}

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct Job {
    pub(crate) job: hopsworks_api::Job,
}


#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct JobExecution {
    pub(crate) job_execution: hopsworks_api::JobExecution,
}

impl From<hopsworks_api::Project> for Project {
    fn from(project: hopsworks_api::Project) -> Self {
        Self { project }
    }
}

impl From<Project> for hopsworks_api::Project {
    fn from(project: Project) -> Self {
        project.project
    }
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

pub(crate) fn register_module(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<Project>()?;
    parent.add_class::<Job>()?;
    parent.add_class::<JobExecution>()?;

    Ok(())
}

