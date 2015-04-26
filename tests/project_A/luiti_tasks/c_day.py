# -*-coding:utf-8-*-

from .__init_luiti import cached_property, TaskDay


class CDay(TaskDay):

    root_dir = "/foobar"

    @cached_property
    def count(self):
        return 3
