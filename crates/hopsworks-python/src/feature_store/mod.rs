use pyo3::prelude::*;

pub mod feature_group;
pub mod feature_view;
pub mod query;

use crate::tokio;

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
    fn get_feature_group(&self, name: &str, version: Option<i32>) -> PyResult<Option<feature_group::FeatureGroup>> {
        let fg = tokio().block_on(self.fs.get_feature_group(name, version)).unwrap();
        Ok(fg.map(feature_group::FeatureGroup::from))
    }

    fn get_feature_view(&self, name: &str, version: Option<i32>) -> PyResult<Option<feature_view::FeatureView>> {
        let fv = tokio().block_on(self.fs.get_feature_view(name, version)).unwrap();
        Ok(fv.map(feature_view::FeatureView::from))
    }
}



pub(crate) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_class::<FeatureStore>()?;
    parent.add_class::<feature_group::FeatureGroup>()?;
    parent.add_class::<feature_view::FeatureView>()?;
    parent.add_class::<query::Query>()?;

    Ok(())
}

