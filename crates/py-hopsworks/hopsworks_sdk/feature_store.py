from __future__ import annotations

from typing import Optional, Union, List

from hopsworks_sdk.hopsworks_rs import PyFeatureStore
from hopsworks_sdk import feature_group, feature_view, query

class FeatureStore:
    _fs : PyFeatureStore

    def __init__(self):
        raise NotImplementedError("Feature Store objects cannot be instantiated via init")
    
    @classmethod
    def _from_pyfs(cls, fs: PyFeatureStore) -> FeatureStore:
        fs_obj = FeatureStore.__new__(FeatureStore)
        fs_obj._fs = fs
        return fs_obj
    
    def get_feature_group(self, name: str, version: Optional[int]) -> feature_group.FeatureGroup:
        return feature_group.FeatureGroup._from_pyfg(self._fs.get_feature_group(name, version if version else 1))

    def create_feature_group(self, name: str, version: Optional[int], description: Optional[str], primary_key: Union[str, List[str]], event_time: Optional[str] = None, online_enabled: bool = False) -> feature_group.FeatureGroup:
        if isinstance(primary_key, str):
            primary_key = [primary_key]
        return feature_group.FeatureGroup._from_pyfg(
            self._fs.create_feature_group(
                name=name,
                version=version if version else 1,
                description=description,
                primary_key=primary_key,
                event_time=event_time,
                online_enabled=online_enabled,
            )
        )

    def get_or_create_feature_group(self, name: str, version: Optional[int], description: Optional[str], primary_key: Optional[List[str]], event_time: Optional[str] = None, online_enabled: bool = False) -> feature_group.FeatureGroup:
        if not primary_key:
            primary_key = []
        elif isinstance(primary_key, str):
            primary_key = [primary_key]

        return feature_group.FeatureGroup._from_pyfg(
            self._fs.get_or_create_feature_group(
                name=name,
                version=version if version else 1,
                description=description,
                primary_key=primary_key,
                event_time=event_time,
                online_enabled=online_enabled,
            )
        )
    
    def get_feature_view(self, name: str, version: Optional[int] = None) -> feature_group.FeatureGroup:
        return feature_view.FeatureView._from_pyfv(self._fs.get_feature_view(name, version if version else 1))
    
    def create_feature_view(self, name: str, version: int, query: query.Query, description: Optional[str] = None) -> feature_view.FeatureView:
        return feature_view.FeatureView._from_pyfv(
            self._fs.create_feature_view(
                name=name,
                version=version,
                query=query._pyquery,
                description=description,
            )
        )