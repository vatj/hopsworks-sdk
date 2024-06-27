use lazy_static::lazy_static;
use pyo3::prelude::*;
use log::debug;

pub mod feature_store;
pub mod platform;

use platform::project::Project;
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

#[pyclass]
#[derive(Clone)]
pub struct HopsworksLoginOptions {
    pub(crate) builder: HopsworksClientBuilder,
}

#[pymethods]
impl HopsworksLoginOptions {
    #[new]
    fn new() -> Self {
        Self {
            builder: HopsworksClientBuilder::new(),
        }
    }
}

#[pyfunction]
pub fn login(options: Option<HopsworksLoginOptions>) -> platform::project::Project {
    let project = tokio().block_on(hopsworks_api::login(options.map(|o| o.builder))).unwrap();
    debug!("Logged in to project: {}", project.name());
    debug!("{:#?}", project);
    Project::from(project)
}

// #[pyfunction]
// pub fn connect_mysql_rondb(uri_database: &str) {
//     tokio().block_on(hopsworks_api::online_store::connect_to_mysql_rondb(uri_database)).unwrap();
//     debug!("Connected to MySQL/RonDB: {}", uri_database);
// }

// #[pyfunction]
// pub fn get_single_feature_vector(queries: Vec<String>) {
//     tokio().block_on(hopsworks_api::online_store::get_single_feature_vector(&queries)).unwrap();
//     debug!("Fetched single feature vector");
// }

// #[pyfunction]
// pub fn get_multiple_feature_vectors(queries: Vec<String>) {
//     tokio().block_on(hopsworks_api::online_store::get_multiple_feature_vectors(&queries)).unwrap();
//     debug!("Fetched multiple feature vectors");
// }

#[pymodule]
fn hopsworks_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    refresh_logger();

    feature_store::register_module(m)?;
    platform::register_module(m)?;
    m.add_wrapped(wrap_pyfunction!(version))?;
    m.add_class::<HopsworksLoginOptions>()?;
    m.add_wrapped(wrap_pyfunction!(login))?;
    m.add_wrapped(wrap_pyfunction!(refresh_logger))?;
    // m.add_wrapped(wrap_pyfunction!(connect_mysql_rondb))?;
    // m.add_wrapped(wrap_pyfunction!(get_single_feature_vector))?;
    // m.add_wrapped(wrap_pyfunction!(get_multiple_feature_vectors))?;
    Ok(())
}