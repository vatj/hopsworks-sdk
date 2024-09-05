from __future__ import annotations

from hopsworks_sdk.hopsworks_rs import PyJob


class Job:
    _pyjob: PyJob

    @property
    def name(self) -> str:
        return self._pyjob.name
