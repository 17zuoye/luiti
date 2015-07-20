# -*-coding:utf-8-*-

__all__ = ["as_a_luiti_task"]

import luigi
from ..luigi_extensions import TaskBase, TaskInit

# Extensions to luigi.Task
task_base_members = [k1 for k1 in sorted(TaskBase.__dict__.keys()) if not k1.startswith("__")]
task_base_members = [k1 for k1 in task_base_members if not k1.startswith("_abc")]
""" member list, see details at TaskBase
>>> ['_persist_files', '_ref_tasks', 'data_dir', 'data_file', 'data_name', 'date_str', 'date_type', 'date_value', 'date_value_by_type_in_begin', 'date_value_by_type_in_end', 'date_value_by_type_in_last', 'errput', 'instances_by_date_range', 'is_external', 'is_reach_the_edge', 'output', 'package_name', 'pre_task_by_self', 'requires', 'reset_date', 'root_dir', 'run', 'run_mode', 'task_class', 'task_clsname', 'task_namespace']
"""


def as_a_luiti_task(**opts):  # Decorator
    """
    Luigi's contrib are really Great, luiti would like to Reuse them through just a decorator.

    Usage:

        @luigi.as_a_luiti_task()
        class AnotherHiveDay(HiveQueryTask):
            pass


    https://github.com/spotify/luigi/tree/master/luigi/contrib
    """

    def func(task_cls):
        """ Main reason is to fix not overwrite `__init__` function. """
        # Make sure it's a luigi.contrib
        assert issubclass(task_cls, luigi.Task), task_cls

        # copy members to target class
        for member in task_base_members:
            setattr(task_cls, member, getattr(TaskBase, member))

        # let `isinstance` works for this wrap task class
        class wrap_cls(TaskBase, task_cls):
            def __init__(self, *args, **kwargs):
                super(wrap_cls, self).__init__(*args, **kwargs)
                TaskInit.setup(self)

        wrap_cls.__doc__ = task_cls.__doc__
        wrap_cls.__module__ = task_cls.__module__
        task_cls = wrap_cls

        return task_cls
    return func
