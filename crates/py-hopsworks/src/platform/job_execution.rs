use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[pyclass]
#[repr(transparent)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PyJobExecution {
    pub(crate) job_execution: hopsworks_api::JobExecution,
}

#[pymethods]
impl PyJobExecution {
    fn job_name(&self) -> String {
        self.job_execution.job_name().to_string()
    }

    fn id(&self) -> i32 {
        self.job_execution.id()
    }

    fn state(&self) -> String {
        self.job_execution.state().to_string()
    }

    fn submission_time(&self) -> String {
        self.job_execution.submission_time().to_string()
    }

    fn get_current_state(&self) -> PyResult<String> {
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        Ok(
            hopsworks_api::blocking::job_execution::get_current_state_blocking(
                &self.job_execution,
                multithreaded,
            )?
            .to_string(),
        )
    }

    fn download_logs(&self, local_dir: Option<String>) -> PyResult<()> {
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        hopsworks_api::blocking::job_execution::download_logs_blocking(
            &self.job_execution,
            local_dir.as_deref(),
            multithreaded,
        )?;
        Ok(())
    }

    fn delete(&self) -> PyResult<()> {
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        hopsworks_api::blocking::job_execution::delete_blocking(
            &self.job_execution,
            multithreaded,
        )?;
        Ok(())
    }

    fn stop(&self) -> PyResult<()> {
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        hopsworks_api::blocking::job_execution::stop_blocking(&self.job_execution, multithreaded)?;
        Ok(())
    }

    fn await_termination(&self) -> PyResult<()> {
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        hopsworks_api::blocking::job_execution::await_termination_blocking(
            &self.job_execution,
            multithreaded,
        )?;
        Ok(())
    }
}

impl From<hopsworks_api::JobExecution> for PyJobExecution {
    fn from(job_execution: hopsworks_api::JobExecution) -> Self {
        Self { job_execution }
    }
}

impl From<PyJobExecution> for hopsworks_api::JobExecution {
    fn from(job_execution: PyJobExecution) -> Self {
        job_execution.job_execution
    }
}
