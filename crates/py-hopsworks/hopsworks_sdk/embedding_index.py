from __future__ import annotations

from enum import Enum
from typing import Optional

from hopsworks_sdk.embedding_feature import EmbeddingFeature
from hopsworks_sdk.hopsworks_rs import PyEmbeddingIndex


class SimilarityFunction(Enum):
    COSINE = ("COSINE",)
    L2 = ("L2",)
    DOT_PRODUCT = "DOT_PRODUCT"


class EmbeddingIndex:
    _ei: PyEmbeddingIndex

    def __init__(self, index_name: Optional[str] = None):
        self._ei = PyEmbeddingIndex.new_with_index_name(index_name)

    @classmethod
    def _from_py_embedding_index(cls, ei: PyEmbeddingIndex) -> EmbeddingIndex:
        ei_obj = EmbeddingIndex.__new__(EmbeddingIndex)
        ei_obj._ei = ei
        return ei_obj

    def add_embedding(
        self,
        name: str,
        dimension: int,
        _similarity_function: Optional[SimilarityFunction] = SimilarityFunction.L2,
    ) -> None:
        self._ei.add_embedding_feature(name, dimension)

    def get_embedding(self, name: str) -> EmbeddingFeature:
        return EmbeddingFeature._from_py_embedding_feature(
            self._ei.get_embedding_feature(name)
        )
