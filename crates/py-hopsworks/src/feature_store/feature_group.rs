use tracing::debug;
use pyo3::prelude::*;
use arrow::pyarrow::ToPyArrow;
use pyo3_polars::PyDataFrame;
use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};
use crate::platform::job_execution::PyJobExecution;

use super::query::PyQuery;

#[pyclass]
#[repr(transparent)]
#[derive(Serialize, Deserialize, Clone, Debug)]
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
    fn name(&self) -> PyResult<String> {
        Ok(self.fg.name().to_string())
    }

    fn version(&self) -> PyResult<i32> {
        Ok(self.fg.version())
    }

    fn description(&self) -> PyResult<Option<String>> {
        Ok(self.fg.description().map(|s| s.to_string()))
    }

    fn primary_key(&self) -> PyResult<Vec<String>> {
        Ok(self.fg.primary_keys()?.iter().map(|s| s.to_string()).collect())
    }

    fn event_time(&self) -> PyResult<Option<String>> {
        Ok(self.fg.event_time().map(|s| s.to_string()))
    }

    fn select(&self, features: Vec<String>) -> PyResult<PyQuery> {
        let features: Vec<&str> = features.iter().map(|s| s.as_str()).collect();
        let query = self.fg.select(&features)?;
        Ok(PyQuery::from(query))
    }

    fn register_feature_group(&mut self, df: PyDataFrame) -> PyResult<()> {
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        let schema = df.0.schema();
        let registered_fg = hopsworks_api::blocking::feature_group::register_feature_group_if_needed_blocking(&self.fg, schema, multithreaded)?;
        if let Some(fg) = registered_fg {
            self.fg = fg;
            debug!("Registered Feature Group: {:?}", self.fg);
        }
        Ok(())
    }

    fn delete(&self) -> PyResult<()> {
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        hopsworks_api::blocking::feature_group::delete_blocking(&self.fg, multithreaded)?;
        Ok(())
    }

    #[cfg(feature="read_arrow_flight_offline_store")]
    fn read_polars_from_offline_store(&self) -> PyResult<PyDataFrame> {
        let before = std::time::Instant::now();
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        let df = hopsworks_api::offline_store::read_polars_from_offline_feature_store_blocking(&self.fg, None, multithreaded)?;
        debug!("Reading from offline store via rust took: {:?}", before.elapsed());
        Ok(PyDataFrame(df))
    }

    #[cfg(feature="read_arrow_flight_offline_store")]
    fn read_arrow_from_offline_store(&self, py: Python) -> PyResult<PyObject> {
        let before = std::time::Instant::now();
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        let batches = hopsworks_api::offline_store::read_arrow_from_offline_feature_store_blocking(&self.fg , None, multithreaded)?;
        debug!("Reading from offline store via rust took: {:?}", before.elapsed());
        batches.to_pyarrow(py)
        // let schema = batches.first().unwrap().schema().to_pyarrow(py);
        // let table: PyObject = py.import_bound("pyarrow")?.getattr("Table")?.call_method1("from_batches", (batches.to_pyarrow(py).iter(), schema))?.into();
        // Ok(table)
    }

    #[cfg(feature="read_sql_online_store")]
    fn read_arrow_from_sql_online_store(&self, py: Python) -> PyResult<PyObject> {
        let before = std::time::Instant::now();
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        let (batches, _) = hopsworks_api::online_store::read_arrow_from_online_store_via_sql_blocking(&self.fg, multithreaded)?;
        debug!("Reading from online store via rust took: {:?}", before.elapsed());
        batches.to_pyarrow(py)
    }

    #[cfg(feature="read_sql_online_store")]
    fn read_polars_from_sql_online_store(&self) -> PyResult<PyDataFrame> {
        let before = std::time::Instant::now();
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        let df = hopsworks_api::online_store::read_polars_from_online_store_via_sql_blocking(&self.fg, multithreaded)?;
        debug!("Reading from online store via rust took: {:?}", before.elapsed());
        Ok(PyDataFrame(df))
    }

    #[cfg(feature="insert_into_kafka")]
    fn insert_polars_df_into_kafka(&mut self, py: Python<'_>, df: PyDataFrame) -> PyResult<PyJobExecution> {
        let before = std::time::Instant::now();
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        let mut dataframe: DataFrame = df.into();
        let job_execution = py.allow_threads(move || {
            hopsworks_api::kafka::insert_polars_df_into_kafka_blocking(&mut dataframe, &self.fg, multithreaded)
        });
        debug!("Inserting into Kafka via rust took: {:?}", before.elapsed()); 
        Ok(PyJobExecution::from(job_execution?))
        
    }
}