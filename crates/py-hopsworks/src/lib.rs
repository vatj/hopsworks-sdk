use lazy_static::lazy_static;
use pyo3::prelude::*;
use log::debug;

pub mod feature_store;
pub mod platform;

use platform::project::PyProject;
use hopsworks_api::HopsworksClientBuilder;

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

#[pyfunction]
pub fn login(url: Option<&str>, api_key_value: Option<&str>, project_name: Option<&str>) -> platform::project::PyProject {
    let builder = HopsworksClientBuilder::new_provided_or_from_env(api_key_value, url, project_name);
    let project = tokio().block_on(hopsworks_api::login(Some(builder))).unwrap();
    debug!("Logged in to project: {}", project.name());
    debug!("{:#?}", project);
    PyProject::from(project)
}

#[pymodule]
fn hopsworks_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    refresh_logger();

    feature_store::register_module(m)?;
    platform::register_module(m)?;
    m.add_wrapped(wrap_pyfunction!(version))?;
    m.add_wrapped(wrap_pyfunction!(login))?;
    m.add_wrapped(wrap_pyfunction!(refresh_logger))?;
    Ok(())
}