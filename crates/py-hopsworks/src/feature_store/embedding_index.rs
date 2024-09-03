use pyo3::prelude::*;
use pyo3::types::PyType;
use serde::{Deserialize, Serialize};

use super::embedding_feature::PyEmbeddingFeature;

#[pyclass]
#[repr(transparent)]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PyEmbeddingIndex {
    pub(crate) ei: hopsworks_api::EmbeddingIndex,
}

impl From<hopsworks_api::EmbeddingIndex> for PyEmbeddingIndex {
    fn from(embedding_index: hopsworks_api::EmbeddingIndex) -> Self {
        Self {
            ei: embedding_index,
        }
    }
}

impl From<PyEmbeddingIndex> for hopsworks_api::EmbeddingIndex {
    fn from(py_embedding_index: PyEmbeddingIndex) -> Self {
        py_embedding_index.ei
    }
}

#[pymethods]
impl PyEmbeddingIndex {
    #[classmethod]
    fn new_with_index_name(_cls: &Bound<'_, PyType>, index_name: &str) -> Self {
        Self {
            ei: hopsworks_api::EmbeddingIndex::new(index_name),
        }
    }

    fn add_embedding_feature(&mut self, name: &str, dimension: u32) {
        let feat = hopsworks_api::EmbeddingFeature::builder()
            .name(String::from(name))
            .dimension(dimension)
            .build();
        self.ei.add_embedding_feature(name, feat);
    }

    fn get_embedding_feature(&self, name: &str) -> Option<PyEmbeddingFeature> {
        self.ei
            .get_embedding_feature(name)
            .map(|f| f.clone().into())
    }

    fn get_embedding_feature_names(&self) -> Vec<String> {
        self.ei.embedding_feature_names()
    }
}
