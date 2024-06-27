use log::debug;
use pyo3::prelude::*;
use arrow::pyarrow::ToPyArrow;
use hopsworks_api::core::register_feature_group_if_needed;
use pyo3_polars::PyDataFrame;
use pyo3::types::PyDict;

use crate::tokio;

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct PyFeatureGroup {
    pub(crate) fg: hopsworks_api::FeatureGroup,
}



impl From<hopsworks_api::FeatureGroup> for PyFeatureGroup {
    fn from(fg: hopsworks_api::FeatureGroup) -> Self {
        Self { fg }
    }
}

impl From<PyFeatureGroup> for hopsworks_api::FeatureGroup {
    fn from(fg: PyFeatureGroup) -> Self {
        fg.fg
    }
}

#[pymethods]
impl PyFeatureGroup {
    fn register_feature_group(&mut self, df: PyDataFrame) -> PyResult<()> {
        let schema = df.0.schema();
        let registered_fg = tokio().block_on(register_feature_group_if_needed(&self.fg, schema))?;
        if let Some(fg) = registered_fg {
            self.fg = fg;
            debug!("Registered Feature Group: {:?}", self.fg);
        }
        Ok(())
    }

    #[cfg(feature="read_arrow_flight_offline_store")]
    fn read_polars_from_offline_store(&self) -> PyResult<PyDataFrame> {
        let df = tokio().block_on(hopsworks_api::offline_store::read_from_offline_feature_store(&self.fg, None))?;
        Ok(PyDataFrame(df))
    }

    #[cfg(feature="read_arrow_flight_offline_store")]
    fn read_arrow_from_offline_store(&self, py: Python) -> PyResult<PyObject> {
        let batches = tokio().block_on(hopsworks_api::offline_store::read_arrow_from_offline_feature_store(&self.fg , None))?;
        batches.to_pyarrow(py)
        // let schema = batches.first().unwrap().schema().to_pyarrow(py);
        // let table: PyObject = py.import_bound("pyarrow")?.getattr("Table")?.call_method1("from_batches", (batches.to_pyarrow(py).iter(), schema))?.into();
        // Ok(table)
    }

    #[cfg(feature="read_sql_online_store")]
    fn read_arrow_from_sql_online_store(&self, py: Python) -> PyResult<PyObject> {
        let (batches, _) = tokio().block_on(hopsworks_api::online_store::read_arrow_from_online_store_via_sql(&self.fg))?;
        batches.to_pyarrow(py)
    }

    #[cfg(feature="read_sql_online_store")]
    fn read_polars_from_sql_online_store(&self) -> PyResult<PyDataFrame> {
        let df = tokio().block_on(hopsworks_api::online_store::read_polars_from_online_store_via_sql(&self.fg))?;
        Ok(PyDataFrame(df))
    }

    #[cfg(feature="insert_into_kafka")]
    fn insert_polars_df_into_kafka(&mut self, df: PyDataFrame) -> PyResult<()> {
        let mut dataframe: DataFrame = df.into();
        tokio().block_on(hopsworks_api::kafka::insert_polars_df_into_kafka(&mut dataframe, &self.fg))?;
        Ok(())
    }
}