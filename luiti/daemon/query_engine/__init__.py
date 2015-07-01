# -*-coding:utf-8-*-

__all__ = ["Query"]

from .builder import QueryBuilder


class Query(object):
    """
    Use params to query some data from luiti.
    """

    def __init__(self, ptm):
        self.ptm = ptm  # global task and package data.

    def get_env(self, raw_params=dict()):
        """
        Generate all data needed.
        """
        qb = QueryBuilder(self.ptm, raw_params)
        return qb.result
