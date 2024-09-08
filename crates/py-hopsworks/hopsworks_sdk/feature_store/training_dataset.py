from __future__ import annotations

from hopsworks_sdk.hopsworks_rs import PyTrainingDataset


class TrainingDataset:
    _td: PyTrainingDataset

    def __init__(self) -> None:
        raise NotImplementedError(
            "Training Dataset cannot be instantiated via init method."
            "First use the Feature Store to create a Feature View object, "
            "then you can use the Feature View methods to create versioned "
            "training data.\n"
            "Note that this object only contains metadata about the training dataset, "
            "it does not encapsulate the corresponding data."
        )

    @classmethod
    def _from_pytd(cls, pytd: PyTrainingDataset) -> TrainingDataset:
        td_obj = TrainingDataset.__new__(TrainingDataset)
        td_obj._td = pytd
        return td_obj

    @property
    def version(self) -> int:
        return self._td.version()
