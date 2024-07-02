use color_eyre::Result;

use hopsworks_core::platform::job_execution::{JobExecution, JobExecutionState};

#[cfg(feature="blocking")]
pub fn get_current_state_blocking(job_exec: &JobExecution, multithreaded: bool) -> Result<JobExecutionState> {
    let rt = hopsworks_core::get_hopsworks_runtime(multithreaded).clone();
    let _guard = rt.enter();

    rt.block_on(job_exec.get_current_state())
}

#[cfg(feature="blocking")]
pub fn stop_blocking(job_exec: &JobExecution, multithreaded: bool) -> Result<JobExecution> {
    let rt = hopsworks_core::get_hopsworks_runtime(multithreaded).clone();
    let _guard = rt.enter();

    rt.block_on(job_exec.stop())
}

#[cfg(feature="blocking")]
pub fn delete_blocking(job_exec: &JobExecution, multithreaded: bool) -> Result<()> {
    let rt = hopsworks_core::get_hopsworks_runtime(multithreaded).clone();
    let _guard = rt.enter();

    rt.block_on(job_exec.delete())
}

#[cfg(feature="blocking")]
pub fn await_termination_blocking(job_exec: &JobExecution, multithreaded: bool) -> Result<()> {
    let rt = hopsworks_core::get_hopsworks_runtime(multithreaded).clone();
    let _guard = rt.enter();

    rt.block_on(job_exec.await_termination())
}

#[cfg(feature="blocking")]
pub fn download_logs_blocking(job_exec: &JobExecution, local_dir: Option<&str>, multithreaded: bool) -> Result<()> {
    let rt = hopsworks_core::get_hopsworks_runtime(multithreaded).clone();
    let _guard = rt.enter();

    rt.block_on(job_exec.download_logs(local_dir))
}