use pyo3::prelude::*;
use query::PyQuery;

pub mod feature_group;
pub mod feature_view;
pub mod query;
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
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        let fg = hopsworks_api::blocking::feature_store::get_feature_group_blocking(&self.fs, name, version, multithreaded)?;
        Ok(fg.map(feature_group::PyFeatureGroup::from))
    }

    fn get_or_create_feature_group(&self, name: &str, version: i32, primary_key: Vec<String>,  online_enabled: bool, description: Option<&str>, event_time: Option<&str>) -> PyResult<feature_group::PyFeatureGroup> {
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        let fg = hopsworks_api::blocking::feature_store::get_or_create_feature_group_blocking(
            &self.fs, 
            name, 
            Some(version), 
            description, 
            primary_key.iter().map(|s| s.as_str()).collect(), 
            event_time, 
            online_enabled, 
            multithreaded,
        )?;
        Ok(feature_group::PyFeatureGroup::from(fg))
    }

    fn get_feature_view(&self, name: &str, version: Option<i32>) -> PyResult<Option<feature_view::PyFeatureView>> {
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        let fv = hopsworks_api::blocking::feature_store::get_feature_view_blocking(&self.fs, name, version, multithreaded)?;
        Ok(fv.map(feature_view::PyFeatureView::from))
    }

    fn create_feature_view(&self, name: &str, version: i32, query: PyQuery, description: Option<&str>) -> PyResult<feature_view::PyFeatureView> {
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        let fv = hopsworks_api::blocking::feature_store::create_feature_view_blocking(
            &self.fs, 
            name, 
            version, 
            query.into(), 
            description, 
            multithreaded,
        )?;
        Ok(feature_view::PyFeatureView::from(fv))
    }
}



pub(crate) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_class::<PyFeatureStore>()?;
    parent.add_class::<feature_group::PyFeatureGroup>()?;
    parent.add_class::<feature_view::PyFeatureView>()?;
    parent.add_class::<query::PyQuery>()?;

    Ok(())
}

