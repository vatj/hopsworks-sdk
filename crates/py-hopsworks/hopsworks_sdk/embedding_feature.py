from __future__ import annotations

from hopsworks_sdk.hopsworks_rs import PyEmbeddingFeature


class EmbeddingFeature:
    _ef: PyEmbeddingFeature

    def __init__(self):
        raise NotImplementedError(
            "Embedding Feature cannot be instantiated via init method."
        )

    @classmethod
    def _from_py_embedding_feature(cls, ef: PyEmbeddingFeature) -> EmbeddingFeature:
        ef_obj = EmbeddingFeature.__new__(EmbeddingFeature)
        ef_obj._ef = ef
        return ef_obj
