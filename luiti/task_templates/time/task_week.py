# -*-coding:utf-8-*-

__all__ = ['TaskWeek']

from etl_utils import cached_property
from ...luigi_extensions import TaskBase
from ...utils import DateUtils


class TaskWeek(TaskBase):

    @cached_property
    def days_in_week(self):
        return list(DateUtils.days_in_week(self.date_value))

    def requires_with_prev_week(self, ref_task1):
        """ require days in current week, and stat data in previous week """
        total_tasks = [ref_task1(date_value=date1) for date1 in self.days_in_week]

        prev_week_stat_task1 = self.pre_task_by_self
        if isinstance(prev_week_stat_task1, self.task_class):
            total_tasks.append(prev_week_stat_task1)  # If it's not RootTask

        return total_tasks
