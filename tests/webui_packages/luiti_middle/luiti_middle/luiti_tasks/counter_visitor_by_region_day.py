# -*-coding:utf-8-*-

from .__init_luiti import TaskDay, luigi


@luigi.ref_tasks("CleanWebLogDay")
class CounterVisitorByRegionDay(TaskDay):

    def requires(self):
        return self.CleanWebLogDay_task
