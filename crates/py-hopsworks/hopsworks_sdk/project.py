from __future__ import annotations

from hopsworks_sdk.hopsworks_rs import PyProject

from hopsworks_sdk.feature_store import FeatureStore


class Project:
    _proj : PyProject

    def __init__(self):
        raise NotImplementedError("Project cannot be instantiated via init method.")
    
    @classmethod
    def _from_pyproj(cls, proj: PyProject) -> Project:
        proj_obj = Project.__new__(Project)
        proj_obj._proj = proj
        return proj_obj
    
    def get_feature_store(self) -> FeatureStore:
        return FeatureStore._from_pyfs(self._proj.get_feature_store())