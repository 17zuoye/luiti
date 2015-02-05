#-*-coding:utf-8-*-

from .__setup import *

class CDay(TaskDay):

    root_dir = "/foobar"

    @cached_property
    def count(self): return 3
