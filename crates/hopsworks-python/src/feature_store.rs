use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct FeatureStore {
    pub(crate) fs: hopsworks_api::FeatureStore,
}

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct FeatureGroup {
    pub(crate) fg: hopsworks_api::FeatureGroup,
}


#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct FeatureView {
    pub(crate) fv: hopsworks_api::FeatureView,
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

impl From<hopsworks_api::FeatureGroup> for FeatureGroup {
    fn from(fg: hopsworks_api::FeatureGroup) -> Self {
        Self { fg }
    }
}

impl From<FeatureGroup> for hopsworks_api::FeatureGroup {
    fn from(fg: FeatureGroup) -> Self {
        fg.fg
    }
}

impl From<hopsworks_api::FeatureView> for FeatureView {
    fn from(fv: hopsworks_api::FeatureView) -> Self {
        Self { fv }
    }
}

impl From<FeatureView> for hopsworks_api::FeatureView {
    fn from(fv: FeatureView) -> Self {
        fv.fv
    }
}

pub(crate) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_class::<FeatureStore>()?;
    parent.add_class::<FeatureGroup>()?;
    parent.add_class::<FeatureView>()?;

    Ok(())
}

