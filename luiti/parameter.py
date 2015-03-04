#-*-coding:utf-8-*-

__all__ = ['ArrowParameter']

import luigi
import arrow
from dateutil import tz

class ArrowParameter(luigi.DateParameter):
    """
    Convert date or time type into Arrow type.

    "2014-11-24T00:00:00+00:00" # => len 25
    "2014-11-24"                # => len 10
    """

    arrow = arrow # make a ref

    def parse(self, s):
        """ overwrite default implement. """
        s = str(s)         # ensure `s` is a str
        assert len(s) in [25, 10], "Date format must be 2014-11-24T00:00:00+00:00 or 2014-11-24 !"
        return arrow.get(s)


    @staticmethod
    def get(*strs):
        """ 把原始的 `arrow.get` 兼容 tzlocal """
        return arrow.get(*strs).replace(tzinfo=tz.tzlocal())

    @staticmethod
    def now():
        return ArrowParameter.get(arrow.now())
