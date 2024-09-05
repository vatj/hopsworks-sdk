from __future__ import annotations

from hopsworks_sdk.hopsworks_rs import PyStorageConnector


class StorageConnector:
    _pysc: PyStorageConnector

    def __init__(self):
        raise NotImplementedError(
            "Storage Connector cannot be instantiated via init method."
        )

    @classmethod
    def _from_pysc(cls, pysc: PyStorageConnector) -> StorageConnector:
        sc_obj = StorageConnector.__new__(StorageConnector)
        sc_obj._pysc = pysc
        return sc_obj
