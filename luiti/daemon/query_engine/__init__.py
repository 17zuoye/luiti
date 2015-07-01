# -*-coding:utf-8-*-

__all__ = ["Query"]

from .builder import QueryBuilder


class Query(object):
    """
    Use params to query some data from luiti.
    """

    cache = dict()

    def __init__(self, ptm):
        self.ptm = ptm  # global task and package data.

    def get_env(self, raw_params=dict()):
        """
        Generate all data needed.
        """
        # TODO cache maybe replaced by a decorator, such as @functools.lru_cache
        cache_key = unicode(sorted(raw_params.items()))  # A simple cache

        result = self.cache.get(cache_key, None)
        if result is None:
            result = QueryBuilder(self.ptm, raw_params).result
            self.cache[cache_key] = QueryBuilder(self.ptm, raw_params).result

        return result
