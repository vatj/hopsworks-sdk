use pyo3::prelude::*;

pub mod feature_group;
pub mod feature_view;
pub mod query;

use crate::tokio;

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct PyFeatureStore {
    pub(crate) fs: hopsworks_api::FeatureStore,
}

impl From<hopsworks_api::FeatureStore> for PyFeatureStore {
    fn from(fs: hopsworks_api::FeatureStore) -> Self {
        Self { fs }
    }
}

impl From<PyFeatureStore> for hopsworks_api::FeatureStore {
    fn from(fs: PyFeatureStore) -> Self {
        fs.fs
    }
}

#[pymethods]
impl PyFeatureStore {
    fn get_feature_group(&self, name: &str, version: Option<i32>) -> PyResult<Option<feature_group::PyFeatureGroup>> {
        let fg = tokio().block_on(self.fs.get_feature_group(name, version)).unwrap();
        Ok(fg.map(feature_group::PyFeatureGroup::from))
    }

    fn get_or_create_feature_group(&self, name: &str, version: i32, primary_key: Vec<String>,  online_enabled: bool, description: Option<&str>, event_time: Option<&str>) -> PyResult<feature_group::PyFeatureGroup> {
        let fg = tokio().block_on(self.fs.get_or_create_feature_group(name, Some(version), description, primary_key.iter().map(String::as_ref).collect(), event_time, online_enabled)).unwrap();
        Ok(feature_group::PyFeatureGroup::from(fg))
    }

    fn get_feature_view(&self, name: &str, version: Option<i32>) -> PyResult<Option<feature_view::PyFeatureView>> {
        let fv = tokio().block_on(self.fs.get_feature_view(name, version)).unwrap();
        Ok(fv.map(feature_view::PyFeatureView::from))
    }
}



pub(crate) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_class::<PyFeatureStore>()?;
    parent.add_class::<feature_group::PyFeatureGroup>()?;
    parent.add_class::<feature_view::PyFeatureView>()?;
    parent.add_class::<query::PyQuery>()?;

    Ok(())
}

