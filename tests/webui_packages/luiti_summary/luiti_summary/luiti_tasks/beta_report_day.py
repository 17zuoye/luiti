# -*-coding:utf-8-*-

from .__init_luiti import TaskDay, luigi


@luigi.ref_tasks("CounterVisitorByBrowserDay", "CounterVisitorByRegionDay", "CounterVisitorDay")
class BetaReportDay(TaskDay):

    def requires(self):
        return [self.CounterVisitorByBrowserDay_task,
                self.CounterVisitorByRegionDay_task,
                self.CounterVisitorDay_task]
