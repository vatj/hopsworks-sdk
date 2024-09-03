use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[pyclass]
#[repr(transparent)]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PyStorageConnector {
    pub(crate) sc: hopsworks_api::StorageConnector,
}

impl From<hopsworks_api::StorageConnector> for PyStorageConnector {
    fn from(sc: hopsworks_api::StorageConnector) -> Self {
        Self { sc }
    }
}

impl From<PyStorageConnector> for hopsworks_api::StorageConnector {
    fn from(sc: PyStorageConnector) -> Self {
        sc.sc
    }
}
