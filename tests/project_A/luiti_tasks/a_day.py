# -*-coding:utf-8-*-

from .__init_luiti import luigi, TaskDay, cached_property


@luigi.ref_tasks("BDay", "CDay")
class ADay(TaskDay):

    root_dir = "/foobar"

    def requires(self):
        return [self.BDay_task, self.CDay_task]

    @cached_property
    def count(self):
        return 1

    @cached_property
    def total_count(self):
        return self.count + self.BDay_task.count + self.CDay_task.count
