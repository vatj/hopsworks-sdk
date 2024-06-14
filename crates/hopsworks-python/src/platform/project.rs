use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct Project {
    pub(crate) project: hopsworks_api::Project,
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




