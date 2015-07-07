# -*-coding:utf-8-*-

from .__init_luiti import TaskDay, luigi


@luigi.ref_tasks("CounterVisitorByBrowserDay")
class CounterVisitorDay(TaskDay):

    def requires(self):
        return self.CounterVisitorByBrowserDay_task
