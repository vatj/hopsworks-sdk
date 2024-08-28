from __future__ import annotations

from hopsworks_sdk.hopsworks_rs import PyQuery

class Query:
    _query : PyQuery

    def __init__(self):
        raise NotImplementedError(
            "Query objects cannot be instantiated via init method"
            "Use `.select*` methods on Feature Group instances to choose features for your query."
            "To mix features from multiple Feature Groups, use `join` method on query objects."
        )
    
    @classmethod
    def _from_pyquery(cls, query: PyQuery) -> Query:
        query_obj = Query.__new__(Query)
        query_obj._query = query
        return query_obj
    
    def join(self) -> Query:
        raise NotImplementedError("Join method is not implemented yet.")
    
