#-*-coding:utf-8-*-

__all__ = ['TaskBase']

import os
import re
from collections  import defaultdict
import json
import arrow
from inflector    import Inflector
from etl_utils    import cached_property
from dateutil     import tz

from ...luigi_ext        import luigi
from ..other.root_task   import RootTask
from ...utils            import DateUtils, ExtUtils, IOUtils
from ...parameter        import ArrowParameter
from ...manager          import luiti_config


class TaskBase(luigi.Task, ExtUtils.ExtendClass):
    """ 继承的子类在类名后 必须加 **时间类型**, 如 Day, Week, ... """

    run_mode           = ["local", "mr_distribute", "mr_local"][0]

    date_value         = ArrowParameter() # **统一** 时间类型, 防止同时跑多个任务
    orig_date_value    = None

    # will overwritten by @decorator
    # 不能以 **两个 __ 开头**, 否则会被 Python 当作隐私变量而无法继承。TODO 隐私变量 可能是错的。
    _persist_files    = []
    _ref_tasks        = []

    root_dir           = NotImplemented

    # Default one, always return True
    def requires(self):
        return RootTask()

    def run     (self):
        raise NotImplementedError

    def __init__(self, *args, **kwargs):
        super(TaskBase, self).__init__(*args, **kwargs)

        # 在跨期的时候用于判断 该周应该是该周的哪些天。
        # 比如这学期开学是 2015-02-17(星期二) 开学, 那么这周的数据只有 0217-0222。
        # 而在寒假里(即run 2015-02-16(星期天) 的 task 时，那么该周的天只有 0216 一天。
        self.orig_date_value = ArrowParameter.get(self.date_value).replace(tzinfo=tz.tzlocal())

        self.reset_date() # reset date to at the beginning of current date type here
        self.data_file      # force load it now, or `output` still load it.
        self.package_name   # force load it now, use to serialize


    @cached_property
    def data_dir(self):
        assert self.root_dir
        return os.path.join(self.root_dir, self.date_str)

    @cached_property
    def data_file(self):
        return os.path.join(self.data_dir, self.data_name + ".json")

    @cached_property
    def data_name(self):
        return Inflector().underscore(self.__class__.__name__)

    def output(self):
        return IOUtils.local_target(self.data_file)

    def errput(self):
        return IOUtils.local_target(self.data_file + ".err")

    @cached_property
    def date_str(self):
        return self.date_value.strftime("%Y-%m-%d")

    @cached_property
    def date_type(self):
        return luiti_config.get_date_type(self.__class__.__name__)

    @cached_property
    def date_value_by_type_in_last(self):
        return DateUtils.date_value_by_type_in_last(self.date_value, self.date_type)

    @cached_property
    def date_value_by_type_in_begin(self):
        return ArrowParameter.get(self.date_value).floor(self.date_type)

    @cached_property
    def date_value_by_type_in_end(self):
        return ArrowParameter.get(self.date_value).ceil(self.date_type)

    @cached_property
    def pre_task_by_self(self):
        """ 如果跨了两个周期就没有上次数据文件了 """
        return RootTask() if self.is_reach_the_edge else self.__class__(self.date_value_by_type_in_last)

    @cached_property
    def is_reach_the_edge(self):
        return False # default. e.g. add semester

    def reset_date(self):
        # **强制** 写为统一时间格式(arrow格式)，这样luigi就不会同时跑两个任务了。
        self.date_value = ArrowParameter.get(self.date_value)

        orig_date       = self.date_value
        if self.date_type != 'range':
            new_date = orig_date.floor(self.date_type)
            if orig_date != new_date:
                print "[reset date by %s] from %s to %s" % (self.date_type, orig_date, new_date)
                self.date_value = new_date


    @classmethod
    def instances_by_date_range(cls, first_date, last_date):
        """ 返回属于某周期里的所有当前任务实例列表 """
        assert isinstance(first_date, arrow.Arrow)
        assert isinstance(last_date,  arrow.Arrow)

        if "Range" in cls.__name__:
            return list(set([cls(first_date), cls(last_date)])) # return head and tail directly
        else:
            dates = arrow.Arrow.range(luiti_config.get_date_type(cls.__name__), first_date, last_date)
            return [cls(date1.datetime) for date1 in dates]

    @cached_property
    def task_class(self):
        return self.__class__

    @cached_property
    def package_name(self):
        module_name  = self.task_class.__module__
        package_name = module_name.split(".")[0]
        return package_name
