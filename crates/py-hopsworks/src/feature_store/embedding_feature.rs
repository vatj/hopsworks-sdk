use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[pyclass]
#[repr(transparent)]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PyEmbeddingFeature {
    pub(crate) ef: hopsworks_api::EmbeddingFeature,
}

impl From<hopsworks_api::EmbeddingFeature> for PyEmbeddingFeature {
    fn from(embedding_feature: hopsworks_api::EmbeddingFeature) -> Self {
        Self {
            ef: embedding_feature,
        }
    }
}

impl From<PyEmbeddingFeature> for hopsworks_api::EmbeddingFeature {
    fn from(py_embedding_feature: PyEmbeddingFeature) -> Self {
        py_embedding_feature.ef
    }
}
