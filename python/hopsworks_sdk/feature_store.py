from hopsworks_sdk.hopsworks_rs import PyFeatureStore

from typing import Optional



class FeatureStore:
    _fs : PyFeatureStore

    def __init__(self):
        raise NotImplementedError("Feature Store objects cannot be instantiated via init")
    
    def get_feature_group(self, name: str, version: Optional[int]) -> FeatureGroup:
        self._fs.get_feature_group(name, version if version else 1)