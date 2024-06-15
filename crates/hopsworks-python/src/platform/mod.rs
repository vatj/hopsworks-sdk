use pyo3::prelude::*;

pub mod project;
pub mod job;
pub mod job_execution;

pub(crate) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_class::<project::Project>()?;
    parent.add_class::<job::Job>()?;
    parent.add_class::<job_execution::JobExecution>()?;

    Ok(())
}