#-*-coding:utf-8-*-

__all__ = ["TaskWeekHadoop"]

from .task_week import *

from ...utils import MRUtils
import json
import re
import luigi

class TaskWeekHadoop(luigi.hadoop.HadoopExt, TaskWeek):

    def requires_with_prev_week(self, ref_task1):
        """ 依赖的天数据，以及上周的自身类统计数据 """
        total_tasks = [ref_task1(date1) for date1 in self.days_in_week]
        prev_week_stat_task1 = self.pre_task_by_self
        if isinstance(prev_week_stat_task1, self.task_class):
            total_tasks.append(prev_week_stat_task1) # 如果不是 RootTask 的话
        return total_tasks

