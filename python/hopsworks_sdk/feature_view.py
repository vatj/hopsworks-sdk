from __future__ import annotations

from hopsworks_sdk.hopsworks_rs import PyFeatureView

class FeatureView:
    _fv: PyFeatureView

    def __init__(self) -> None:
        raise NotImplementedError("Feature View cannot be instantiated via init method.")
    
    @classmethod
    def _from_pyfv(cls, pyfv: PyFeatureView) -> FeatureView:
        fv_obj = FeatureView.__new__(FeatureView)
        fv_obj._fv = pyfv
        return fv_obj