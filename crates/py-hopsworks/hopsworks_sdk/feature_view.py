from __future__ import annotations
from typing import Any, Dict

from hopsworks_sdk.hopsworks_rs import PyFeatureView

class FeatureView:
    _fv: PyFeatureView

    def __init__(self) -> None:
        raise NotImplementedError(
            "Feature View cannot be instantiated via init method."
            "Use the Feature Store instance methods to create a new or get an existing"
            "FeatureView."
        )
    
    @classmethod
    def _from_pyfv(cls, pyfv: PyFeatureView) -> FeatureView:
        fv_obj = FeatureView.__new__(FeatureView)
        fv_obj._fv = pyfv
        return fv_obj
    
    def init_online_store_rest_client(self, api_key: str, api_version: str = "0.1.0") -> None:
        self._fv.init_online_store_rest_client(api_key=api_key, api_version=api_version)

    def get_feature_vector(self, entry: Dict[str, Any], use_rest: bool = True) -> Dict[str, Any]:
        if use_rest:
            return self._fv.get_feature_vector(entry=entry, _passed_values=None, _rest_read_options=None)
        else:
            raise NotImplementedError("Only REST client is supported for get_feature_vector.")
    
    def delete(self) -> None:
        self._fv.delete()

    @property
    def name(self) -> str:
        return self._fv.name()

    @property
    def version(self) -> int:
        return self._fv.version()