#-*-coding:utf-8-*-

from luiti import TaskDay, cached_property

class HDay(TaskDay):

    @cached_property
    def count(self): return 8
