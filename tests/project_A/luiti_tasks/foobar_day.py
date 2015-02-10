#-*-coding:utf-8-*-

from .__init_luiti import *

class FoobarDay(TaskDayHadoop):

    root_dir = "/foobar"


    def mapper(self, line1):
        d2 = json.loads(line1)
        yield d2['uid'], d2

    def reducer(self, uid1, d1):
        yield '', MRUtils.str_dump({
                    "uid": uid1,
                    "total": sum([i2['count'] for i2 in d1]),
                    "ref": self.ref,
                })


    def mrtest_input(self):
        return u"""
{"uid": 3, "count": 2}
{"uid": 3, "count": 3}
"""

    def mrtest_output(self):
        return u"""
{"uid": 3, "total": 5, "ref": "foobar"}
"""


    def mrtest_attrs(self):
        return {
                "ref" : "foobar",
            }
