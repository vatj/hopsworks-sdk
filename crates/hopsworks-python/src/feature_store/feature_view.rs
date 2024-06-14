use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct FeatureView {
    pub(crate) fv: hopsworks_api::FeatureView,
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

