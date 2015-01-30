#-*-coding:utf-8-*-

from .__setup import *

class CDay(TaskDay):

    @cached_property
    def count(self): return 3
