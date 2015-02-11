#-*-coding:utf-8-*-

__all__ = ['TaskRange']

from .task_base import TaskBase, luigi
from ...utils  import DateUtils


class TaskRange(TaskBase):

    # NOTE date_value 和 date_range 两个值是必须的。
    # 1. date_value 是写到那个日期目录
    # 2. date_range 是指定了依赖的日期范围

    def date_range(self): raise ValueError("Overwrite Me!")
    # date_range = luigi.DateIntervalParameter()
    # date_range = luigi.Parameter() # 临时现为 str 类型吧


    @property
    def dates_in_range(self):
        # method_1 = self.date_type + "s_in_range" # e.g. weeks_in_range
        method_1 = 'week' + "s_in_range" # NOTE 目前直接为 week, 因为是range.

# s1 = "2014-10-01-2014-10-07"
# s1[0:10]  => '2014-10-01'
# s1[11:21] => '2014-10-07'
        date_1, date_2 = self.date_range[0:10], self.date_range[11:21]

        return list(getattr(DateUtils, method_1)(date_1, date_2))
