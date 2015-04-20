#-*-coding:utf-8-*-

__all__ = ['TaskWeek']

from .task_base import *
from ...utils   import DateUtils
from etl_utils  import cached_property


class TaskWeek(TaskBase):

    @cached_property
    def days_in_week(self):
        return list(DateUtils.days_in_week(self.date_value))
