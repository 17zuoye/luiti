# -*-coding:utf-8-*-


__all__ = ["CreateTask"]

import luigi
from ..utils import CacheByDictKey


class CreateTask(object):

    task_clsname_cache = dict()

    @staticmethod
    def new(task_cls, _params):
        """ Initialize a task instance, with filter invalid params. """
        task_cls_cache = CreateTask.task_clsname_cache.get(task_cls, None)
        if task_cls_cache is None:
            task_cls_cache = TaskInstanceCache(task_cls)
            CreateTask.task_clsname_cache[task_cls] = task_cls_cache

        return task_cls_cache[_params]


class TaskInstanceCache(object):
    """
    To avoid create duplicated task instances.
    """

    def __init__(self, task_cls):
        self.task_cls = task_cls
        self.cache = CacheByDictKey(self.process)

    def __getitem__(self, _params):
        return self.cache[_params]

    def process(self, _params):
        _real_task_params = dict()
        for k1, v1 in _params.iteritems():
            has_key = hasattr(self.task_cls, k1)
            is_luigi_params = isinstance(getattr(self.task_cls, k1, None), luigi.Parameter)
            if has_key and is_luigi_params:
                _real_task_params[k1] = v1
        task_instance = self.task_cls(**_real_task_params)
        return task_instance
