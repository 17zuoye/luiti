# -*-coding:utf-8-*-

from .__init_luiti import TaskDay
from etl_utils import cached_property


class DumpBrowserMapDay(TaskDay):
    """
    Mimic dump {int: name} format data from MySQL relational database.
    """

    @cached_property
    def cached_data(self):
        """
        Actually need to read data from self.output().
        """
        return {
            "Google Chrome": 1,
            "Mozilla Firefox": 2,
            "IE": 3,
        }
