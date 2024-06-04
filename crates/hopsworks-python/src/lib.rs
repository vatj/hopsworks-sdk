pub mod pylib {
    use lazy_static::lazy_static;
    use pyo3::prelude::*;
    lazy_static! {
        static ref LOG_RESET_HANDLE: pyo3_log::ResetHandle = pyo3_log::init();
    }
    #[pyfunction]
    pub fn version() -> &'static str {
        hopsworks_api::VERSION
    }

    #[pyfunction]
    pub fn refresh_logger() {
        LOG_RESET_HANDLE.reset();
    }

    fn hopsworks_sdk(_py: Python<'_>) -> PyResult<()> {
        refresh_logger();

        let m = PyModule::new_bound(_py, "hopsworks_sdk")?;

        m.add_wrapped(wrap_pyfunction!(version))?;
        m.add_wrapped(wrap_pyfunction!(refresh_logger))?;
        Ok(())
    }
}