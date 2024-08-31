from __future__ import annotations

from hopsworks_sdk.hopsworks_rs import PyJobExecution


class JobExecution:
    _job_exec: PyJobExecution

    def __init__(self):
        raise NotImplementedError(
            "Job Execution cannot be instantiated via init method."
        )

    @classmethod
    def _from_pyjobexec(cls, job_exec: PyJobExecution) -> JobExecution:
        job_exec_obj = JobExecution.__new__(JobExecution)
        job_exec_obj._job_exec = job_exec
        return job_exec_obj

    def await_termination(self) -> None:
        self._job_exec.await_termination()
