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
    fn name(&self) -> String {
        self.project.name().to_string()
    }

    fn id(&self) -> i32 {
        self.project.id()
    }

    fn get_feature_store(&self) -> PyResult<PyFeatureStore> {
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        let fs = hopsworks_api::blocking::project::get_feature_store_blocking(
            &self.project,
            multithreaded,
        )?;
        Ok(PyFeatureStore::from(fs))
    }

    #[cfg(feature = "opensearch")]
    fn init_hopsworks_opensearch_client(&self) -> PyResult<()> {
        hopsworks_api::opensearch::init_hopsworks_opensearch_client_blocking(
            self.id(),
            *crate::MULTITHREADED.get().unwrap_or(&true),
        )?;
        Ok(())
    }
}
