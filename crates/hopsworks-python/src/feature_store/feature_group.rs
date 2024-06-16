use pyo3::prelude::*;
#[cfg(feature="read_arrow_flight_offline_store")]
use polars::prelude::DataFrame;
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
    #[cfg(feature="read_arrow_flight_offline_store")]
    fn read_polars_from_offline_store(&self, offline_read_options: Option<ArrowFlightReadOptions>) -> PyResult<DataFrame> {
        let df = read_from_offline_feature_store(&self.fg, offline_read_options).await?;
        Ok(df)
    }

    #[cfg(feature="read_arrow_flight_offline_store")]
    fn read_arrow_from_offline_store(&self, offline_read_options: Option<ArrowFlightReadOptions>) -> PyResult<()> {
        
    }
}