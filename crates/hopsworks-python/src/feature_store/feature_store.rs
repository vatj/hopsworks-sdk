use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::feature_store::feature_group::FeatureGroup;
use crate::feature_store::feature_view::FeatureView;

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct FeatureStore {
    pub(crate) fs: hopsworks_api::FeatureStore,
}

impl From<hopsworks_api::FeatureStore> for FeatureStore {
    fn from(fs: hopsworks_api::FeatureStore) -> Self {
        Self { fs }
    }
}

impl From<FeatureStore> for hopsworks_api::FeatureStore {
    fn from(fs: FeatureStore) -> Self {
        fs.fs
    }
}

#[pymethods]
impl FeatureStore {
    fn get_feature_group(&self, name: &str, version: Option<i32>) -> PyResult<FeatureGroup> {
        let fg = tokio().block_on(self.fs.get_feature_group(name, version))?;
        Ok(FeatureGroup::from(fg))
    }

    fn get_feature_view(&self, name: &str, version: Option<i32>) -> PyResult<FeatureView> {
        let fv = tokio().block_on(self.fs.get_feature_view(name, version))?;
        Ok(FeatureView::from(fv))
    }
}



