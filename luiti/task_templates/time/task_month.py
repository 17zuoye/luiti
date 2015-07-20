# -*-coding:utf-8-*-

__all__ = ['TaskMonth']

from etl_utils import cached_property
from ...luigi_extensions import TaskBase
import arrow


class TaskMonth(TaskBase):

    @cached_property
    def days_in_month(self):
            return arrow.Arrow.range(
                'day',
                self.date_value.floor('month'),
                self.date_value.ceil('month'),)