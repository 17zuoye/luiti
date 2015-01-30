#-*-coding:utf-8-*-

from .__setup import *

class BDay(TaskDay):

    @cached_property
    def count(self): return 2
