use pyo3::prelude::*;

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct Query {
    pub(crate) query: hopsworks_api::Query,
}