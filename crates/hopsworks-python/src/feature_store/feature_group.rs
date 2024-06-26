use log::debug;
use pyo3::prelude::*;
// #[cfg(feature="read_arrow_flight_offline_store")]
// use polars::prelude::DataFrame;
// #[cfg(feature="read_arrow_flight_offline_store")]
use arrow::pyarrow::ToPyArrow;
use hopsworks_api::offline_store::read_from_offline_feature_store;
use hopsworks_api::offline_store::read_arrow_from_offline_feature_store;
use hopsworks_api::online_store::read_arrow_from_online_store_via_sql;
use hopsworks_api::online_store::read_polars_from_online_store_via_sql;
use hopsworks_api::kafka::insert_polars_df_into_kafka;
use hopsworks_api::core::register_feature_group_if_needed;
use pyo3_polars::PyDataFrame;
use polars::prelude::DataFrame;
use pyo3::types::PyDict;

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
    fn register_feature_group(&mut self, df: PyDataFrame) -> PyResult<()> {
        let schema = df.0.schema();
        let registered_fg = tokio().block_on(register_feature_group_if_needed(&self.fg, schema))?;
        if let Some(fg) = registered_fg {
            self.fg = fg;
            debug!("Registered Feature Group: {:?}", self.fg);
        }
        Ok(())
    }

    // #[cfg(feature="read_arrow_flight_offline_store")]
    fn read_polars_from_offline_store(&self) -> PyResult<PyDataFrame> {
        let df = tokio().block_on(read_from_offline_feature_store(&self.fg, None))?;
        Ok(PyDataFrame(df))
    }

    // #[cfg(feature="read_arrow_flight_offline_store")]
    fn read_arrow_from_offline_store(&self, py: Python) -> PyResult<PyObject> {
        let batches = tokio().block_on(read_arrow_from_offline_feature_store(&self.fg , None))?;
        batches.to_pyarrow(py)
        // let schema = batches.first().unwrap().schema().to_pyarrow(py);
        // let table: PyObject = py.import_bound("pyarrow")?.getattr("Table")?.call_method1("from_batches", (batches.to_pyarrow(py).iter(), schema))?.into();
        // Ok(table)
    }

    fn read_arrow_from_sql_online_store(&self, py: Python) -> PyResult<PyObject> {
        let (batches, _) = tokio().block_on(read_arrow_from_online_store_via_sql(&self.fg))?;
        batches.to_pyarrow(py)
    }

    fn read_polars_from_sql_online_store(&self) -> PyResult<PyDataFrame> {
        let df = tokio().block_on(read_polars_from_online_store_via_sql(&self.fg))?;
        Ok(PyDataFrame(df))
    }

    fn insert_polars_df_into_kafka(&mut self, df: PyDataFrame) -> PyResult<()> {
        let mut dataframe: DataFrame = df.into();
        tokio().block_on(insert_polars_df_into_kafka(&mut dataframe, &self.fg)).unwrap();
        Ok(())
    }
}