use pyo3::prelude::*;

use crate::feature_store::PyFeatureStore;

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct PyProject {
    pub(crate) project: hopsworks_api::Project,
}

impl From<hopsworks_api::Project> for PyProject {
    fn from(project: hopsworks_api::Project) -> Self {
        Self { project }
    }
}

impl From<PyProject> for hopsworks_api::Project {
    fn from(project: PyProject) -> Self {
        project.project
    }
}

#[pymethods]
impl PyProject {
    fn name(&self) -> PyResult<String> {
        Ok(self.project.name().to_string())
    }

    fn id(&self) -> PyResult<i32> {
        Ok(self.project.id())
    }

    fn get_feature_store(&self) -> PyResult<PyFeatureStore> {
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        let fs = hopsworks_api::blocking::project::get_feature_store_blocking(&self.project, multithreaded)?;
        Ok(PyFeatureStore::from(fs))
    }
}




