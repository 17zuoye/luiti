#-*-coding:utf-8-*-

from .__init_luiti import *

class BDay(TaskDay):

    root_dir = "/foobar"

    @cached_property
    def count(self):
        return 2
