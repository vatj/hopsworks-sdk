use pyo3::prelude::*;

use crate::feature_store::FeatureStore;
use crate::tokio;

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

#[pymethods]
impl Project {
    fn get_feature_store(&self) -> PyResult<FeatureStore> {
        let fs = tokio().block_on(self.project.get_feature_store()).unwrap();
        Ok(FeatureStore::from(fs))
    }
}




