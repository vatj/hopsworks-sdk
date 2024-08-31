use pyo3::prelude::*;
use std::{sync::OnceLock, time::Duration};
use tracing::debug;
use tracing_subscriber::prelude::*;

pub mod feature_store;
pub mod platform;

use hopsworks_api::HopsworksClientBuilder;
use platform::project::PyProject;

static LOG_RESET_HANDLE: OnceLock<pyo3_log::ResetHandle> = OnceLock::new();
static MULTITHREADED: OnceLock<bool> = OnceLock::new();

#[pyfunction]
pub fn version() -> &'static str {
    hopsworks_api::VERSION
}

#[pyfunction]
pub fn set_multithreaded(multithreaded: bool) {
    if MULTITHREADED.get().is_none() {
        MULTITHREADED.get_or_init(|| multithreaded);
    } else {
        MULTITHREADED.set(multithreaded).unwrap();
    }
}

#[pyfunction]
pub fn refresh_logger() {
    if LOG_RESET_HANDLE.get().is_none() {
        LOG_RESET_HANDLE.get_or_init(pyo3_log::init);
    }
    LOG_RESET_HANDLE.get().unwrap().reset();
}

#[pyfunction]
pub fn login(
    url: Option<&str>,
    api_key_value: Option<&str>,
    project_name: Option<&str>,
    multithreaded: Option<bool>,
) -> PyResult<platform::project::PyProject> {
    let multithreaded = multithreaded.unwrap_or(true);
    let builder =
        HopsworksClientBuilder::new_provided_or_from_env(api_key_value, url, project_name);
    let project = hopsworks_api::login_blocking(Some(builder), multithreaded)?;
    debug!("Logged in to project: {}", project.name());
    debug!("{:#?}", project);
    Ok(PyProject::from(project))
}

#[pyfunction]
pub fn init_subscriber() {
    let console_layer = console_subscriber::ConsoleLayer::builder()
        .retention(Duration::from_secs(60))
        .spawn();

    tracing_subscriber::registry()
        .with(console_layer)
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Initialized subscriber");
}

#[pymodule]
fn hopsworks_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // refresh_logger();
    set_multithreaded(true);

    feature_store::register_module(m)?;
    platform::register_module(m)?;
    m.add_wrapped(wrap_pyfunction!(version))?;
    m.add_wrapped(wrap_pyfunction!(login))?;
    m.add_wrapped(wrap_pyfunction!(refresh_logger))?;
    m.add_wrapped(wrap_pyfunction!(init_subscriber))?;
    Ok(())
}
