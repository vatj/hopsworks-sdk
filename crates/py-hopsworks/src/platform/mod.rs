use pyo3::prelude::*;

pub mod job;
pub mod job_execution;
pub mod project;

pub(crate) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_class::<project::PyProject>()?;
    parent.add_class::<job::PyJob>()?;
    parent.add_class::<job_execution::PyJobExecution>()?;

    Ok(())
}
