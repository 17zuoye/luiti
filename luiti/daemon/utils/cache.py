# -*-coding:utf-8-*-

__all__ = ["CacheByDictKey"]

# TODO cache maybe replaced by a decorator, such as @functools.lru_cache
# 1. https://pypi.python.org/pypi/py_lru_cache/0.1.4 is slow, 100 ms, but simple dict cache is only 1 ms.
# 2. https://github.com/tkem/cachetools dont support dict parameters.


class CacheByDictKey(object):
    """
    Support cache by a dict.

    Only support dict[] operation.
    """

    def __init__(self, func):
        self.store = dict()

        assert callable(func)
        self.func = func

    def __getitem__(self, query):
        cache_key = self.generate_cache_key(query)

        result = self.store.get(cache_key, None)
        if result is None:
            result = self.func(query)
            self.store[cache_key] = result
        return result

    def generate_cache_key(self, query):
        assert isinstance(query, dict)
        return unicode(sorted(query.items()))
