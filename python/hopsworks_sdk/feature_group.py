from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Union

from hopsworks_sdk.hopsworks_rs import PyFeatureGroup

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa


class FeatureGroup:
    _fg : PyFeatureGroup

    def __init__(self):
        raise NotImplementedError("Feature Group cannot be instantiated via init method.")
    
    @classmethod
    def _from_pyfg(cls, fg: PyFeatureGroup) -> FeatureGroup:
        fg_obj = FeatureGroup.__new__(FeatureGroup)
        fg_obj._fg = fg
        return fg_obj
    
    def save(self, dataframe: pl.DataFrame) -> None:
        self._fg.register_feature_group(dataframe)

    def read_from_offline_store(self, return_type: Literal["polars", "pyarrow"] = "polars") -> Union[pl.DataFrame, pa.RecordBatch]:
        if return_type.lower() == "polars":
            df = self._fg.read_polars_from_offline_store()
        elif return_type.lower() == "pyarrow":
            df = self._fg.read_arrow_from_offline_store()
        else:
            raise NotImplementedError("""Supported return type are `"polars"` and `"pyarrow`".""")
        
        return df
    
    def read_from_online_store(self, return_type: Literal["polars", "pyarrow"] = "polars", client: Literal["sql", "rest"] = "sql") -> Union[pl.DataFrame, pa.RecordBatch]:
        if client.lower() == "rest":
            raise NotImplementedError("Rest client is not implemented for large read_operations.")
        
        if return_type.lower() == "polars":
            df = self._fg.read_polars_from_sql_online_store()
        elif return_type.lower() == "pyarrow":
            df = self._fg.read_arrow_from_sql_online_store()
        else:
            raise NotImplementedError("""Supported return type are `"polars"` and `"pyarrow`".""")
        
        return df
    
    def insert(self, dataframe: pl.DataFrame) -> None:
        self._fg.insert_polars_df_into_kafka(dataframe)

