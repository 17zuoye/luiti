# -*-coding:utf-8-*-

from .__init_luiti import luigi, cached_property, TaskDay


@luigi.ref_tasks("FoobarDay")
class CDay(TaskDay):

    root_dir = "/foobar"

    def requires(self):
        self.FoobarDay_task

    @cached_property
    def count(self):
        return 3
