from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

from hopsworks_sdk.hopsworks_rs import PyFeatureView


if TYPE_CHECKING:
    import polars as pl

    TrainingDatasetDataFrameTypes = pl.DataFrame
    StatisticsConfig = Dict[str, Any]

    from hopsworks_sdk import job, storage_connector


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

    def init_online_store_rest_client(
        self, api_key: str, api_version: str = "0.1.0"
    ) -> None:
        self._fv.init_online_store_rest_client(api_key=api_key, api_version=api_version)

    def get_feature_vector(
        self, entry: Dict[str, Any], use_rest: bool = True
    ) -> Dict[str, Any]:
        if use_rest:
            return self._fv.get_feature_vector(
                entry=entry, _passed_values=None, _rest_read_options=None
            )
        else:
            raise NotImplementedError(
                "Only REST client is supported for get_feature_vector."
            )

    def get_batch_data(
        self,
        start_time: Optional[Union[str, int, datetime, date]] = None,
        end_time: Optional[Union[str, int, datetime, date]] = None,
        primary_key: bool = False,
        event_time: bool = False,
        inference_helper_columns: bool = False,
        transformed: bool = False,
    ) -> pl.DataFrame:
        return self._fv.get_batch_data(
            start_time=start_time,
            end_time=end_time,
            primary_key=primary_key,
            event_time=event_time,
            inference_helper_columns=inference_helper_columns,
            transformed=transformed,
        )

    def create_training_data(
        self,
        start_time: Optional[Union[str, int, datetime, date]] = "",
        end_time: Optional[Union[str, int, datetime, date]] = "",
        storage_connector: Optional[storage_connector.StorageConnector] = None,
        location: Optional[str] = "",
        description: Optional[str] = "",
        extra_filter: Optional[Union[filter.Filter, filter.Logic]] = None,
        data_format: Optional[str] = "parquet",
        coalesce: Optional[bool] = False,
        seed: Optional[int] = None,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        write_options: Optional[Dict[Any, Any]] = None,
        primary_key: bool = False,
        event_time: bool = False,
        training_helper_columns: bool = False,
        **kwargs,
    ) -> Tuple[int, job.Job]:
        raise NotImplementedError("Not implemented yet")

    def create_train_test_split(
        self,
        test_size: Optional[float] = None,
        train_start: Optional[Union[str, int, datetime, date]] = "",
        train_end: Optional[Union[str, int, datetime, date]] = "",
        test_start: Optional[Union[str, int, datetime, date]] = "",
        test_end: Optional[Union[str, int, datetime, date]] = "",
        storage_connector: Optional[storage_connector.StorageConnector] = None,
        location: Optional[str] = "",
        description: Optional[str] = "",
        extra_filter: Optional[Union[filter.Filter, filter.Logic]] = None,
        data_format: Optional[str] = "parquet",
        coalesce: Optional[bool] = False,
        seed: Optional[int] = None,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        write_options: Optional[Dict[Any, Any]] = None,
        primary_key: bool = False,
        event_time: bool = False,
        training_helper_columns: bool = False,
        **kwargs,
    ) -> Tuple[int, job.Job]:
        raise NotImplementedError("Not implemented yet")

    def create_train_validation_test_split(
        self,
        validation_size: Optional[float] = None,
        test_size: Optional[float] = None,
        train_start: Optional[Union[str, int, datetime, date]] = "",
        train_end: Optional[Union[str, int, datetime, date]] = "",
        validation_start: Optional[Union[str, int, datetime, date]] = "",
        validation_end: Optional[Union[str, int, datetime, date]] = "",
        test_start: Optional[Union[str, int, datetime, date]] = "",
        test_end: Optional[Union[str, int, datetime, date]] = "",
        storage_connector: Optional[storage_connector.StorageConnector] = None,
        location: Optional[str] = "",
        description: Optional[str] = "",
        extra_filter: Optional[Union[filter.Filter, filter.Logic]] = None,
        data_format: Optional[str] = "parquet",
        coalesce: Optional[bool] = False,
        seed: Optional[int] = None,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        write_options: Optional[Dict[Any, Any]] = None,
        primary_key: bool = False,
        event_time: bool = False,
        training_helper_columns: bool = False,
        **kwargs,
    ) -> Tuple[int, job.Job]:
        raise NotImplementedError("Not implemented yet")

    def training_data(
        self,
        start_time: Optional[Union[str, int, datetime, date]] = None,
        end_time: Optional[Union[str, int, datetime, date]] = None,
        description: Optional[str] = "",
        extra_filter: Optional[Union[filter.Filter, filter.Logic]] = None,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        read_options: Optional[Dict[Any, Any]] = None,
        primary_key: bool = False,
        event_time: bool = False,
        training_helper_columns: bool = False,
        dataframe_type: Optional[str] = "default",
        **kwargs,
    ) -> Tuple[
        TrainingDatasetDataFrameTypes,
        Optional[TrainingDatasetDataFrameTypes],  # optional label DataFrame
    ]:
        raise NotImplementedError("Not implemented yet")

    def train_test_split(
        self,
        test_size: Optional[float] = None,
        train_start: Optional[Union[str, int, datetime, date]] = "",
        train_end: Optional[Union[str, int, datetime, date]] = "",
        test_start: Optional[Union[str, int, datetime, date]] = "",
        test_end: Optional[Union[str, int, datetime, date]] = "",
        description: Optional[str] = "",
        extra_filter: Optional[Union[filter.Filter, filter.Logic]] = None,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        read_options: Optional[Dict[Any, Any]] = None,
        primary_key: bool = False,
        event_time: bool = False,
        training_helper_columns: bool = False,
        dataframe_type: Optional[str] = "default",
        **kwargs,
    ) -> Tuple[
        TrainingDatasetDataFrameTypes,
        TrainingDatasetDataFrameTypes,
        Optional[TrainingDatasetDataFrameTypes],
        Optional[TrainingDatasetDataFrameTypes],
    ]:
        raise NotImplementedError("Not implemented yet")

    def train_validation_test_split(
        self,
        validation_size: Optional[float] = None,
        test_size: Optional[float] = None,
        train_start: Optional[Union[str, int, datetime, date]] = "",
        train_end: Optional[Union[str, int, datetime, date]] = "",
        validation_start: Optional[Union[str, int, datetime, date]] = "",
        validation_end: Optional[Union[str, int, datetime, date]] = "",
        test_start: Optional[Union[str, int, datetime, date]] = "",
        test_end: Optional[Union[str, int, datetime, date]] = "",
        description: Optional[str] = "",
        extra_filter: Optional[Union[filter.Filter, filter.Logic]] = None,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        read_options: Optional[Dict[Any, Any]] = None,
        spine: Optional[SplineDataFrameTypes] = None,
        primary_key: bool = False,
        event_time: bool = False,
        training_helper_columns: bool = False,
        dataframe_type: Optional[str] = "default",
        **kwargs,
    ) -> Tuple[
        TrainingDatasetDataFrameTypes,
        TrainingDatasetDataFrameTypes,
        TrainingDatasetDataFrameTypes,
        Optional[TrainingDatasetDataFrameTypes],
        Optional[TrainingDatasetDataFrameTypes],
        Optional[TrainingDatasetDataFrameTypes],
    ]:
        raise NotImplementedError("Not implemented yet")

    def delete(self) -> None:
        self._fv.delete()

    @property
    def name(self) -> str:
        return self._fv.name()

    @property
    def version(self) -> int:
        return self._fv.version()
