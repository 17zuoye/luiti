#-*-coding:utf-8-*-

__all__ = ['luigi']

import luigi
from luigi import Event
import luigi.hadoop
luigi.hadoop = luigi.hadoop
import luigi.hdfs
luigi.hdfs = luigi.hdfs

from .hadoop_ext import HadoopExt
luigi.hadoop.HadoopExt = HadoopExt # write back
# NOTE 对 luigi.hadoop 兼容 "track the job: "


import os
import arrow
from .parameter import ArrowParameter
import importlib
from inflector import Inflector
from etl_utils import cached_property

from .utils import IOUtils, TargetUtils
from .manager import Loader
luigi.HDFS = TargetUtils.hdfs # 本来就是需要读取全局配置，所以索性就绑定在 luigi 命名空间了吧。

def persist_files(*files): # 装饰器
    """ 多个data_file 可以用 DSL 描述，然后和 event_handler(Event.FAILURE) 绑定在一起 """
    def func(cls):
        # 1. 设置 持久化文件属性
        def wrap(file1): # 这样才可以保存 file1 变量，而不至于被覆写。
            def _file(self):
                return os.path.join(self.data_dir, file1 + ".json")
            return _file

        cls.__persist_files = files
        for file1 in cls.__persist_files:
            setattr(cls, file1, property(wrap(file1))) # @decorator

        # 2. 绑定 失败时删除这些文件
        def clean_tmp(task, exception):
            for file1 in files: IOUtils.remove_files(getattr(task, file1))
            # IOUtils.remove_files(task.data_file) # NOTE 好像 Hadoop 会自动处理失败任务的输出文件的，否则就会导致其在N次重试一直在running。
        cls.event_handler(Event.FAILURE)(clean_tmp)

        return cls

    return func
luigi.persist_files = persist_files



def ref_tasks(*tasks): # 装饰器
    """
    自动把依赖 Task 链接起来，通过属性访问。

    Example:

    ```python
    @ref_tasks("TaskB", "TaskC")
    class TaskA(TaskWeekBase):
        pass

    TaskA().TaskB == TaskB
    TaskA().TaskC == TaskC
    ```
    """
    def wrap(ref_task_name):
        return lambda self: Loader.load_a_task_by_name(ref_task_name)

    def func(cls):
        curr_task_name = cls.__name__
        cls._ref_tasks = tasks
        for ref_task_name in cls._ref_tasks:
            #print curr_task_name, "[ref on]", ref_task_name # 不要在 MapReduce 里 print，否则会输出到 output 的
            #setattr(cls, ref_task_name, cached_property(wrap(ref_task_name)))
            setattr(cls, ref_task_name, property(wrap(ref_task_name)))
        return cls
    return func
luigi.ref_tasks = ref_tasks



def check_date_range(): # 装饰器
    """
    从数据库导数据时，必须注意时间范围内的所有数据是否都齐全了。如果未齐全，
    即在当前时间范围里导的话，那么就会缺失数据了，相当于提前导了。

    比如在周六就把这周的关联数据导出来，那么周日的数据就没包含在里面。应该在下周一后才开始导。
    """
    def decorator(orig_run):
        def new_run(self):
            # 说明时间未到，然后就直接退出
            if self.date_value_by_type_in_begin <= ArrowParameter.now() < self.date_value_by_type_in_end:
                return False
            return orig_run(self)
        return new_run

    def func(cls):
        cls.run = decorator(cls.run)
        return cls
    return func
luigi.check_date_range = check_date_range


def check_runtime_range(**opts_1): # 装饰器
    """
    Support hour/weekday indexed range.

    Optional params:
    1. hour_num
    2. weekday_num
    3. now
    """
    def decorator(orig_run):
        def new_run(self):
            default_opts = {
                    "hour_num"      : range(1, 25),
                    "weekday_num"   : range(1, 8),
                }
            opts = dict(default_opts.items() + opts_1.items())

            now            = ArrowParameter.now()           # get current time
            hour_24        = int(now.format("H"))  # 0, 1, 2 ... 23, 24
            day_of_week_7  = int(now.format("d"))  # 1, 2, 3 ... 6, 7

            is_false = False
            if hour_24       not in opts['hour_num']:    is_false = True
            if day_of_week_7 not in opts['weekday_num']: is_false = True
            if is_false:
                print "[info]", now, " is not in ", opts, ", so the task exited."
                return False

            return orig_run(self)
        return new_run

    def func(cls):
        cls.run = decorator(cls.run)
        return cls
    return func
luigi.check_runtime_range = check_runtime_range
