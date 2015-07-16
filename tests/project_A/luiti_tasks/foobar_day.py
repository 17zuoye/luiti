# -*-coding:utf-8-*-

from .__init_luiti import TaskDayHadoop, json, MRUtils


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
{"uid": 1, "count": 2}
{"uid": 1, "count": 3}
{"uid": 2, "count": 1}
"""

    def mrtest_output(self):
        return u"""
{"uid": 1, "total": 5, "ref": "foobar"}
{"uid": 2, "total": 1, "ref": "foobar"}
"""

    def mrtest_attrs(self):
        return {
            "ref": "foobar",
        }
