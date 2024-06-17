use pyo3::prelude::*;
// #[cfg(feature="read_arrow_flight_offline_store")]
use polars::prelude::DataFrame;
// #[cfg(feature="read_arrow_flight_offline_store")]
use arrow::record_batch::RecordBatch;
use hopsworks_api::offline_store::ArrowFlightReadOptions;
use hopsworks_api::offline_store::read_from_offline_feature_store;
use hopsworks_api::offline_store::read_arrow_from_offline_feature_store;

use crate::tokio;

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct FeatureGroup {
    pub(crate) fg: hopsworks_api::FeatureGroup,
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

#[pymethods]
impl FeatureGroup {
    // #[cfg(feature="read_arrow_flight_offline_store")]
    fn read_polars_from_offline_store(&self, py: Python) -> PyResult<DataFrame> {
        let df = tokio().block_on(read_from_offline_feature_store(&self.fg, None)).unwrap();
        df
    }

    // #[cfg(feature="read_arrow_flight_offline_store")]
    fn read_arrow_from_offline_store(&self, py: Python) -> PyResult<RecordBatch> {
        let record_batch = tokio().block_on(read_arrow_from_offline_feature_store(&self.fg , None)).unwrap();
        let table_class = py.import_bound("pyarrow")?.getattr("Table")?;
        table_class.call_method1("from_batches", (record_batch, record_batch.schema().into_py(py)))
    }
}