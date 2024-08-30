use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[pyclass]
#[repr(transparent)]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PyEmbeddingIndex {
    pub(crate) ei: hopsworks_api::EmbeddingIndex,
}


impl From<hopsworks_api::EmbeddingIndex> for PyEmbeddingIndex {
    fn from(embedding_index: hopsworks_api::EmbeddingIndex) -> Self {
        Self { ei: embedding_index }
    }
}

impl From<PyEmbeddingIndex> for hopsworks_api::EmbeddingIndex {
    fn from(py_embedding_index: PyEmbeddingIndex) -> Self {
        py_embedding_index.ei
    }
}