use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[pyclass]
#[repr(transparent)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PyTrainingDataset {
    pub(crate) td: hopsworks_api::TrainingDataset,
}

#[pymethods]
impl PyTrainingDataset {
    fn version(&self) -> i32 {
        self.td.version()
    }
}
