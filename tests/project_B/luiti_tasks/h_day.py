# -*-coding:utf-8-*-

from luiti import TaskDay, cached_property, luigi


@luigi.ref_tasks("MultipleDependentDay")
class HDay(TaskDay):

    root_dir = "/foobar"

    @cached_property
    def count(self):
        return 8
