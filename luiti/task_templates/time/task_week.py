# -*-coding:utf-8-*-

__all__ = ['TaskWeek']

from etl_utils import cached_property
from ...luigi_extensions import TaskBase
from ...utils import DateUtils


class TaskWeek(TaskBase):

    @cached_property
    def days_in_week(self):
        return list(DateUtils.days_in_week(self.date_value))
