# -*-coding:utf-8-*-

from .__init_luiti import WebuiDay, luigi


@luigi.ref_tasks("CounterVisitorByBrowserDay")
class CounterVisitorDay(WebuiDay):

    def requires(self):
        return self.CounterVisitorByBrowserDay_task
