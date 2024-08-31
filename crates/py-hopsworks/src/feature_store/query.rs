use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[pyclass]
#[repr(transparent)]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PyQuery {
    pub(crate) query: hopsworks_api::Query,
}

impl From<hopsworks_api::Query> for PyQuery {
    fn from(query: hopsworks_api::Query) -> Self {
        Self { query }
    }
}

impl From<PyQuery> for hopsworks_api::Query {
    fn from(query: PyQuery) -> Self {
        query.query
    }
}
