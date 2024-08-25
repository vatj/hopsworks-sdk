use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[pyclass]
#[repr(transparent)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PyFeatureView {
    pub(crate) fv: hopsworks_api::FeatureView,
}

#[pymethods]
impl PyFeatureView {
    pub fn name(&self) -> PyResult<String> {
        Ok(self.fv.name().to_string())
    }

    pub fn version(&self) -> PyResult<i32> {
        Ok(self.fv.version())
    }

    #[cfg(feature = "read_rest_online_store")]
    pub fn init_online_store_rest_client(&self, api_key: String, api_version: String) -> PyResult<()> {
        let api_key = api_key.as_str();
        let api_version = api_version.as_str();
        let reqwest_client = None; // TODO: Add possibility to pass a reqwest client/builder
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        hopsworks_api::online_store::rest_read::init_online_store_rest_client_blocking(api_key, api_version, reqwest_client, multithreaded)?;
        Ok(())
    }

    #[cfg(feature = "read_rest_online_store")]
    pub fn get_feature_vector(&self, entry: serde_json::Value, passed_values: Option<serde_json::Value>, rest_read_options: Option<PyDict>) -> PyResult<hopsworks_api::online_store::rest_read::SingleFeatureVector> {
        let entry = entry;
        let passed_values = passed_values;
        let rest_read_options = rest_read_options;
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        let sfv = hopsworks_api::online_store::rest_read::get_feature_vector_blocking(self.fv.clone().into(), entry, passed_values, rest_read_options, multithreaded)?;
        Ok(sfv)
    }
}

impl From<hopsworks_api::FeatureView> for PyFeatureView {
    fn from(fv: hopsworks_api::FeatureView) -> Self {
        Self { fv }
    }
}

impl From<PyFeatureView> for hopsworks_api::FeatureView {
    fn from(fv: PyFeatureView) -> Self {
        fv.fv
    }
}

