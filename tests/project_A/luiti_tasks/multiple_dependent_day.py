# -*-coding:utf-8-*-

from .__init_luiti import luigi, TaskDay


@luigi.ref_tasks("FoobarDay")
class MultipleDependentDay(TaskDay):

    root_dir = "/foobar"

    def requires(self):
        return self.FoobarDay_task
