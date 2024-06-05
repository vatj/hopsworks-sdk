use lazy_static::lazy_static;
use pyo3::prelude::*;
use log::debug;

pub mod feature_store;
pub mod platform;

use platform::Project;

lazy_static! {
    static ref LOG_RESET_HANDLE: pyo3_log::ResetHandle = pyo3_log::init();
}

fn tokio() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}


#[pyfunction]
pub fn version() -> &'static str {
    hopsworks_api::VERSION
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
pub fn login(options: Option<HopsworksLoginOptions>) -> platform::Project {
    let project = tokio().block_on(hopsworks_api::login(options.map(|o| o.builder))).unwrap();
    debug!("Logged in to project: {}", project.name());
    debug!("{:#?}", project);
    Project::from(project)
}

#[pymodule]
fn hopsworks_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    refresh_logger();

    feature_store::register_module(m)?;
    platform::register_module(m)?;
    m.add_wrapped(wrap_pyfunction!(version))?;
    m.add_class::<HopsworksLoginOptions>()?;
    m.add_wrapped(wrap_pyfunction!(login))?;
    m.add_wrapped(wrap_pyfunction!(refresh_logger))?;
    Ok(())
}