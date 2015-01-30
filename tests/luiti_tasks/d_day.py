#-*-coding:utf-8-*-

from .__setup import *

@luigi.ref_tasks("HDay")
class DDay(TaskDay):

    @cached_property
    def count(self): return 4


    @cached_property
    def total_count(self):
        return self.count + self.HDay_task.count
