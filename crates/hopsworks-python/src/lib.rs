use lazy_static::lazy_static;
use pyo3::prelude::*;
use log::debug;

pub mod feature_store;
pub mod platform;

use platform::Project;

lazy_static! {
    static ref LOG_RESET_HANDLE: pyo3_log::ResetHandle = pyo3_log::init();
}


#[pyfunction]
pub fn version() -> &'static str {
    hopsworks_api::VERSION
}

/// Prints a message.
#[pyfunction]
fn hello() -> PyResult<String> {
    Ok("Hello from hopsworks-sdk!".into())
}

#[pyfunction]
pub fn refresh_logger() {
    LOG_RESET_HANDLE.reset();
}

#[pyclass]
#[derive(Clone)]
pub struct HopsworksLoginOptions {
    pub(crate) builder: hopsworks_api::HopsworksClientBuilder,
}

#[pymethods]
impl HopsworksLoginOptions {
    #[new]
    fn new() -> Self {
        Self {
            builder: hopsworks_api::HopsworksClientBuilder::new(),
        }
    }
}

#[pyfunction]
pub async fn login(options: Option<HopsworksLoginOptions>) -> platform::Project {
    let project = hopsworks_api::login(options.map(|o| o.builder)).await.unwrap();
    debug!("Logged in to project: {}", project.name());
    Project::from(project)
}

#[pymodule]
fn hopsworks_rs(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    refresh_logger();

    feature_store::register_module(_py, m)?;
    platform::register_module(_py, m)?;
    m.add_wrapped(wrap_pyfunction!(version))?;
    m.add_wrapped(wrap_pyfunction!(hello))?;
    m.add_wrapped(wrap_pyfunction!(refresh_logger))?;
    m.add_class::<HopsworksLoginOptions>()?;
    m.add_wrapped(wrap_pyfunction!(login))?;
    Ok(())
}