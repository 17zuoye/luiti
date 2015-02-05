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


import os, sys
import arrow
from .parameter import ArrowParameter
import importlib
from inflector import Inflector
from etl_utils import cached_property, singleton

from .utils import IOUtils, TargetUtils
from . import manager
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
    def wrap_cls(ref_task_name):
        def _func(self):
            v1 = self.__dict__.get(ref_task_name, None)
            if v1 is None:
                v1 = manager.load_a_task_by_name(ref_task_name)
                self.__dict__[ref_task_name] = v1
            return v1
        return _func

    def wrap_instance(ref_task_name, task_name):
        def _func(self):
            v1 = self.__dict__.get(task_name, None)
            if v1 is None:
                v1 = getattr(self, ref_task_name)(self.date_value)
                self.__dict__[task_name] = v1
            return v1
        return _func

    def __getstate__(self):
        """ Fix luiti_tasks module namespace conflicts. """
        for ref_task1 in self._ref_tasks:
            cname = ref_task1           # class    name
            iname = ref_task1 + "_task" # instance name

            # delete instance property is enough.
            #if hasattr(self.__class__, cname):  delattr(self.__class__, cname)
            #if hasattr(self.__class__, iname):  delattr(self.__class__, iname)

            if cname in self.__dict__:          del self.__dict__[cname]
            if iname in self.__dict__:          del self.__dict__[iname]
        return self.__dict__

# cached_property 捕获不了 ref_task_name 变量, 被重置为某一个了。。
# property 可以捕获 ref_task_name 变量。
    def func(cls):
        cls._ref_tasks = tasks
        for ref_task_name in cls._ref_tasks:
            setattr(cls, ref_task_name, property(wrap_cls(ref_task_name)))

            # TODO 根据当前日期返回。
            task_name = "%s_%s" % (ref_task_name, "task")
            setattr(cls, task_name, property(wrap_instance(ref_task_name, task_name)))

            # clear ref task info when pickle.dump
            setattr(cls, "__getstate__", __getstate__)
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
# TODO support Hadoop
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




@singleton()
class LuitiConfigClass(object):
    """ Make sure init variables only once. """
    curr_project_name = None

    @cached_property
    def attached_package_names(self): return set(['luiti'])

    @cached_property
    def luiti_tasks_packages(self): return set([])


def plug_packages(*package_names):
    """
    let luigi know which packages should be attached, and can send to YARN, etc.

    package format can be any valid Python package name, such as "project_B" or "project_C==0.0.2", etc.
    """
    # 1. Check current work dir is under a valid a luiti_tasks project
    curr_dir = os.getcwd()
    if not os.path.exists(os.path.join(curr_dir, "luiti_tasks")):
        raise ValueError("[error] current work dir [%s] has no luiti_tasks dir!" % curr_dir)

    curr_project_name     = os.path.basename(curr_dir) # "project_A"
    curr_project_syspath  = os.path.dirname(curr_dir)  # project_A/

    # 2. Setup sys.path
    global luigi # Fix UnboundLocalError: local variable `luigi` referenced before assignment
    luigi.luiti_config = LuitiConfigClass()
    luigi.luiti_config.curr_project_name
    if luigi.luiti_config.curr_project_name is not None:
        return "already load main project." # below code will never be executed!
    else:
        luigi.luiti_config.curr_project_name = curr_project_name

    # 2.1. be importable
    if curr_project_syspath not in sys.path: sys.path.insert(0, curr_project_syspath)
    # 2.2. it's the root luiti tasks package
    luigi.luiti_config.luiti_tasks_packages.add(manager.import2(curr_project_name))
    # 2.3. ensure other luiti tasks packages can be loaded.
    manager.import2(curr_project_name + ".luiti_tasks.__init_luiti")


    # 3. Load related packages.
    import pkg_resources
    import luigi.hadoop

    for p1 in package_names:
        package2, version2 = (p1 + "==").split("==")[0:2]
        if package2 in luigi.luiti_config.attached_package_names:
            continue
        else:
            # Notice Python to import special version package.
            if version2: pkg_resources.require(p1)

            # Let luigi know it.
            luigi.hadoop.attach(package2)

            # Add valid package which has .luiti_tasks
            try:
                if manager.import2(package2 + ".luiti_tasks"):
                    luigi.luiti_config.luiti_tasks_packages.add(manager.import2(package2))
            except ImportError as ie:
                pass

luigi.plug_packages = plug_packages
