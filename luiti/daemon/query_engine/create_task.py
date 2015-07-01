# -*-coding:utf-8-*-


__all__ = ["CreateTask"]

import luigi


class CreateTask(object):

    @staticmethod
    def new(task_cls, _params):
        """ Initialize a task instance, with filter invalid params. """
        _real_task_params = dict()
        for k1, v1 in _params.iteritems():
            has_key = hasattr(task_cls, k1)
            is_luigi_params = isinstance(getattr(task_cls, k1, None), luigi.Parameter)
            if has_key and is_luigi_params:
                _real_task_params[k1] = v1
        task_instance = task_cls(**_real_task_params)
        return task_instance
