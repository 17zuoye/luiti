# -*-coding:utf-8-*-

__all__ = ['TaskDay']

from ...luigi_extensions import TaskBase
import arrow
from etl_utils import cached_property

class TaskDay(TaskBase):

    @cached_property
    def latest_7_days(self):
            return arrow.Arrow.range(
                'day',
                self.date_value.replace(days=-6),
                self.date_value,)

    @cached_property
    def latest_30_days(self):
            return arrow.Arrow.range(
                'day',
                self.date_value.replace(days=-29),
                self.date_value,)
