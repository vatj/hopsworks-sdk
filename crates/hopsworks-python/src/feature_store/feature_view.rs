use pyo3::prelude::*;

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct PyFeatureView {
    pub(crate) fv: hopsworks_api::FeatureView,
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

