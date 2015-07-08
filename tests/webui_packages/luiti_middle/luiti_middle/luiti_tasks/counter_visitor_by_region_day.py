# -*-coding:utf-8-*-

from .__init_luiti import WebuiDay, luigi


@luigi.ref_tasks("CleanWebLogDay")
class CounterVisitorByRegionDay(WebuiDay):

    def requires(self):
        return self.CleanWebLogDay_task
